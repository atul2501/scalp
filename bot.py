"""Polymarket crypto Up/Down: from 2 s before each window ends, maker-bid 0.99 on the side the market already
priced >= 0.98 at its last trade, hold to settlement, stop on the first loss.

Why (research.py on Polygemy's history, out-of-sample): this rule won 2,058/2,058 BTC-5m fills in Oct
and 617/617 BTC-15m; bidding before the end lost ~2% of fills (-$28/day at 10 shares).
LIVE=0 (default) is dry-run: public endpoints only, simulated fills with a queue model.
One thread per coin and window length; dashboard on PORT while the bot runs.
"""
import csv, json, os, signal, sys, threading, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket
from dotenv import load_dotenv

load_dotenv()
COINS = ["btc", "eth", "sol", "doge", "xrp", "bnb"]  # hype also exists but its book is too thin
SPANS = {"5m": 300, "15m": 900, "1h": 3600}           # every coin trades each of these windows
SERIES = [f"{c}-{s}" for s in SPANS for c in COINS]   # e.g. "btc-5m", one thread each
HOURLY_NAME = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "doge": "dogecoin", "xrp": "xrp", "bnb": "bnb"}
ENTRY = 0.99        # maker bid price
CONFIRM = 0.98      # the market's last trade before the end must price our side at least this
LEAD = 2            # post this many seconds before the end for queue place; 5-10 s lost on 15m history
GRACE = 90          # seconds after the end the bid may rest; backtest fills: median 2 s, p90 15 s
STOP = None         # after the end a stop only sells winners; None = hold to settlement
SIZE = 10           # shares (market min is 5)
MAX_LOSSES = 1      # the backtest had zero losses: any loss means stop and investigate
POLL = 0.2
PORT = 8000
DASH_HOST = os.getenv("DASH_HOST", "127.0.0.1")  # 0.0.0.0 to reach the dashboard from outside
LIVE = os.getenv("LIVE", "0") == "1"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

client = None
if LIVE:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    client = ClobClient(CLOB, key=os.environ["PRIVATE_KEY"], chain_id=137,
                        signature_type=int(os.getenv("SIGNATURE_TYPE", "1")), funder=os.getenv("FUNDER"))
    client.set_api_creds(client.create_or_derive_api_creds())

state = {"mode": "LIVE" if LIVE else "DRY-RUN", "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "config": {"entry": f"{ENTRY} from {LEAD}s before end if market >= {CONFIRM}", "grace": GRACE, "stop": STOP,
                    "size": SIZE, "max_losses": MAX_LOSSES},
         "wins": 0, "losses": 0, "pnl": 0.0, "windows": 0, "bids": 0, "fills": 0, "net_errors": 0,
         "coins": {k: {"market": None, "position": None} for k in SERIES}, "pending": [], "status": "starting"}
lock = threading.Lock()     # guards counters, pending, trades.csv and status.json
halt = threading.Event()    # kill switch: coin threads finish their window and exit


def bump(key, n=1):
    with lock:
        state[key] = round(state[key] + n, 4)


def save_state():
    with lock:
        state["updated"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with open("status.json.tmp", "w") as f:
            json.dump(state, f)
        os.replace("status.json.tmp", "status.json")


def slug_for(coin, span, ts):
    start = int(ts) // SPANS[span] * SPANS[span]
    if span != "1h":
        return f"{coin}-updown-{span}-{start}"
    # hourly markets are named after the New York hour they start in
    # ponytail: the repeated 1am hour on the DST-end day maps to one slug; that hour may be skipped
    et = datetime.fromtimestamp(start, ZoneInfo("America/New_York"))
    hour = et.strftime("%I").lstrip("0") + et.strftime("%p").lower()
    return f"{HOURLY_NAME[coin]}-up-or-down-{et.strftime('%B').lower()}-{et.day}-{et.year}-{hour}-et"


def pnl(entry, exit_, size):
    return round((exit_ - entry) * size, 4)


def winner(p_up):
    """Side index the market has decided (0 Up, 1 Down) from its last trade price, or None."""
    if p_up is None:
        return None
    return 0 if p_up >= CONFIRM else 1 if p_up <= 1 - CONFIRM else None


def can_rest(ask):
    return ask > ENTRY  # an ask at/below 0.99 would make our bid a taker


def sim_fill(price, ahead, sells, level):
    """Dry-run queue model. Taker sells at our price eat the queue ahead of us first;
    a sell below our price means our whole level was cleared. Returns (ahead, filled)."""
    filled = 0.0
    for p, s in sells:
        if p < price:
            return 0.0, float("inf")
        if p == price:
            eat = min(ahead, s)
            ahead -= eat
            filled += s - eat
    # ponytail: cancels anywhere in the level shrink it; assume they were ahead of us (slightly optimistic)
    return min(ahead, level), filled


def get(url, **params):
    """GET json, retrying forever so a held position is never abandoned on a network blip."""
    attempt = 0
    while True:
        try:
            return requests.get(url, params=params, timeout=5).json()
        except (requests.RequestException, ValueError) as e:
            attempt += 1
            bump("net_errors")
            print(f"  net error ({type(e).__name__}), retry {attempt}")
            time.sleep(min(attempt, 10))


def get_market(slug, closed=False):
    # Gamma hides closed markets unless asked for them explicitly
    r = get(f"{GAMMA}/markets", slug=slug, closed=str(closed).lower())
    if not r:
        return None
    m = r[0]
    return {"slug": slug, "outcomes": json.loads(m["outcomes"]), "tokens": json.loads(m["clobTokenIds"]),
            "end": datetime.fromisoformat(m["endDate"].replace("Z", "+00:00")).timestamp(),
            "prices": json.loads(m["outcomePrices"] or "[]")}


def levels(rows):
    return {float(x["price"]): float(x["size"]) for x in rows}


class Feed:
    """Live order books and trades for one market over the CLOB websocket, REST fallback when quiet."""

    def __init__(self, tokens):
        self.books = {t: ({}, {}) for t in tokens}
        self.sells = {t: [] for t in tokens}
        self.up = tokens[0]
        self.other = {tokens[0]: tokens[1], tokens[1]: tokens[0]}
        self.p_up = None   # last trade of either token, as a price of Up
        self.last = 0.0
        self.alive = True
        self.lock = threading.Lock()
        self.ws = websocket.WebSocketApp(
            WS, on_open=lambda ws: ws.send(json.dumps({"assets_ids": tokens, "type": "market"})),
            on_message=self.on_message)
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        while self.alive:
            self.ws.run_forever(ping_interval=10, ping_timeout=5)
            if self.alive:
                bump("net_errors")
                print("  feed reconnect (websocket dropped)")
                time.sleep(1)

    def close(self):
        self.alive = False
        self.ws.close()

    def on_message(self, ws, raw):
        try:
            msgs = json.loads(raw)
        except ValueError:
            return  # PONG etc.
        with self.lock:
            self.last = time.time()
            for x in msgs if isinstance(msgs, list) else [msgs]:
                kind, tok = x.get("event_type"), x.get("asset_id")
                if kind == "book" and tok in self.books:
                    self.books[tok] = (levels(x["bids"]), levels(x["asks"]))
                elif kind == "price_change":
                    for c in x.get("price_changes", []):
                        if c["asset_id"] in self.books:
                            book = self.books[c["asset_id"]][0 if c["side"] == "BUY" else 1]
                            p, s = float(c["price"]), float(c["size"])
                            if s:
                                book[p] = s
                            else:
                                book.pop(p, None)
                elif kind == "last_trade_price" and tok in self.sells:
                    p = float(x["price"])
                    self.p_up = p if tok == self.up else 1 - p
                    if x.get("side") == "SELL":
                        self.sells[tok].append((p, float(x["size"])))
                    else:  # buying one token at p also fills bids on the other at 1 - p (the exchange mints a pair)
                        self.sells[self.other[tok]].append((round(1 - p, 4), float(x["size"])))

    def top(self, token):
        """(best bid, best ask)."""
        if time.time() - self.last > 5:
            b = get(f"{CLOB}/book", token_id=token)
            with self.lock:
                self.books[token] = (levels(b.get("bids", [])), levels(b.get("asks", [])))
                self.last = time.time()  # quiet books: refresh over REST every 5s, not every poll
        with self.lock:
            bids, asks = self.books[token]
            return max(bids, default=0.0), min(asks, default=1.0)

    def level(self, token, price):
        with self.lock:
            return self.books[token][0].get(price, 0.0)

    def take_sells(self, token):
        with self.lock:
            s, self.sells[token] = self.sells[token], []
            return s


def log(slug, side, entry, exit_, size, reason, when=None):
    p = pnl(entry, exit_, size) if size else 0.0
    when = when or datetime.now(timezone.utc)
    with lock:
        new = not os.path.exists("trades.csv")
        with open("trades.csv", "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["time", "slug", "side", "entry", "exit", "size", "pnl", "reason", "live"])
            w.writerow([when.isoformat(timespec="seconds"), slug, side, entry, exit_, size, p, reason, LIVE])
    print(f"  >> {slug} {reason.upper()} {side} {entry}->{exit_}  pnl {p:+}")
    bump("pnl", p)
    if reason == "win":
        bump("wins")
    elif reason in ("stop", "loss"):
        bump("losses")


def settle_pending():
    """Check held positions from finished windows; Gamma marks the winner some minutes after close."""
    for p in list(state["pending"]):
        r = get_market(p["slug"], closed=True)
        if r and r["prices"] and float(r["prices"][p["idx"]]) in (0.0, 1.0):
            payout = float(r["prices"][p["idx"]])
            closed_at = datetime.fromtimestamp(p["end"], timezone.utc)
            log(p["slug"], p["side"], p["entry"], payout, p["held"], "win" if payout == 1.0 else "loss", closed_at)
            with lock:
                state["pending"].remove(p)


def serve_dashboard():
    class Dashboard(SimpleHTTPRequestHandler):
        PUBLIC = {"/": "/dashboard.html", "/dashboard.html": "/dashboard.html",
                  "/status.json": "/status.json", "/trades.csv": "/trades.csv"}

        def do_GET(self):
            # only these files; never .env (private key) or anything else in the folder
            path = self.PUBLIC.get(self.path.split("?")[0])
            if not path:
                return self.send_error(404)
            self.path = path
            super().do_GET()

        def log_message(self, *a):
            pass
    try:
        srv = ThreadingHTTPServer((DASH_HOST, PORT), Dashboard)
    except OSError as e:
        print(f"dashboard disabled: {e}")
        return
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f"dashboard: http://{DASH_HOST}:{PORT}/")


def trade_window(key, m):
    """One window of one series: watch until the end, then bid the market-confirmed winner for GRACE
    seconds. A filled position goes to state['pending'] and settles from Gamma."""
    assert m["outcomes"][0] == "Up"
    view = state["coins"][key]
    feed = Feed(m["tokens"])
    order_id, side_idx, held, ahead, presigned = None, None, 0.0, 0.0, None
    sold = 0.0  # shares sold into our price after we posted (dry-run diagnostics)
    stopped = False
    if LIVE:
        for t in m["tokens"]:  # cache tick size / neg-risk now so posting at the end is one request
            client.get_tick_size(t)
            client.get_neg_risk(t)
    try:
        while time.time() < m["end"] + GRACE and not halt.is_set():
            books = [feed.top(t) for t in m["tokens"]]
            left = m["end"] - time.time()
            view["market"] = {"slug": m["slug"], "outcomes": m["outcomes"], "books": books, "seconds_left": int(left),
                              "p_up": feed.p_up}

            if side_idx is None and LIVE and LEAD < left <= LEAD + 3 and presigned is None:
                i = winner(feed.p_up)
                if i is not None:  # sign ahead so the end costs only the HTTP post
                    presigned = (i, client.create_order(OrderArgs(token_id=m["tokens"][i], price=ENTRY, size=SIZE, side=BUY)))

            if side_idx is None and left <= LEAD:
                i = winner(feed.p_up)
                if i is not None and can_rest(books[i][1]):
                    side_idx, tok = i, m["tokens"][i]
                    ahead = feed.level(tok, ENTRY)
                    feed.take_sells(tok)  # only trades after we post count toward our fill
                    bump("bids")
                    view["position"] = {"side": m["outcomes"][i], "status": "bidding", "held": 0,
                                        "entry": ENTRY, "ahead": round(ahead, 2)}
                    print(f"  {key} BID {m['outcomes'][i]} {SIZE}@{ENTRY}  queue ahead {ahead:.0f}  {-left:+.1f}s vs end")
                    if LIVE:
                        o = presigned[1] if presigned and presigned[0] == i else client.create_order(
                            OrderArgs(token_id=tok, price=ENTRY, size=SIZE, side=BUY))
                        resp = client.post_order(o, OrderType.GTC, post_only=True)
                        order_id = resp.get("orderID")
                        if not order_id:
                            print(f"  rejected: {resp}")

            elif side_idx is not None and held < SIZE:  # waiting on (more) fill
                tok = m["tokens"][side_idx]
                bid, ask = books[side_idx]
                was = held
                if LIVE and order_id:
                    held = float(client.get_order(order_id).get("size_matched", 0))
                elif not LIVE:
                    sells = feed.take_sells(tok)
                    sold += sum(sz for p, sz in sells if p <= ENTRY)
                    ahead, filled = sim_fill(ENTRY, ahead, sells, feed.level(tok, ENTRY))
                    if ask <= ENTRY:  # a seller resting at our price means our level was cleared
                        filled = SIZE
                    held = min(SIZE, held + filled)
                    view["position"]["ahead"] = round(ahead, 2)
                if held > was:
                    if not was:
                        bump("fills")
                    view["position"].update(status="filled", held=held)
                    print(f"  {key} FILLED {held} @ {ENTRY}  {-left:.1f}s after end")

            if held and STOP is not None:
                bid, _ = books[side_idx]
                if bid <= STOP:
                    if LIVE:
                        if order_id:
                            client.cancel(order_id)
                        client.post_order(client.create_market_order(MarketOrderArgs(
                            token_id=m["tokens"][side_idx], amount=held, side=SELL)), OrderType.FAK)
                    log(m["slug"], m["outcomes"][side_idx], ENTRY, bid, held, "stop")
                    stopped = True
                    break
            # wake exactly at the join moment: fills come seconds after the end (backtest median 2 s)
            time.sleep(min(POLL, left - LEAD) if left - LEAD > 0.01 else POLL)
    finally:
        feed.close()
        if LIVE and order_id and not stopped:
            client.cancel(order_id)  # drop any unfilled remainder

    if side_idx is not None and not held:
        print(f"  {key} no fill: {sold:.0f} shares sold at <= {ENTRY} after our bid, {ahead:.0f} still ahead")
        log(m["slug"], m["outcomes"][side_idx], ENTRY, "", 0, "nofill")  # bid never filled: no trade
    if held and not stopped:
        with lock:
            state["pending"].append({"slug": m["slug"], "idx": side_idx, "side": m["outcomes"][side_idx],
                                     "held": held, "entry": ENTRY, "end": m["end"]})
    view["position"] = None


def coin_loop(key):
    """Trade every window of one coin/span series (e.g. "btc-15m") until the kill switch trips."""
    coin, span = key.split("-")
    while not halt.is_set():
        try:
            m = get_market(slug_for(coin, span, time.time()))
            if not m or m["end"] <= time.time():
                time.sleep(5)
                continue
            bump("windows")
            print(f"== {m['slug']} ends {datetime.fromtimestamp(m['end'], timezone.utc):%H:%M:%S}Z")
            trade_window(key, m)  # returns GRACE s after the end; the next window is already open
        except Exception as e:  # one coin's bad window must not kill the others
            print(f"  {coin} error: {e!r}")
            time.sleep(5)


def run():
    print(f"mode={state['mode']} series={len(SERIES)} ({COINS} x {list(SPANS)}) entry={ENTRY} from {LEAD}s before end if market>={CONFIRM} grace={GRACE}s stop={STOP} "
          f"size={SIZE} max_losses={MAX_LOSSES}")
    serve_dashboard()
    state["status"] = "running"
    threads = [threading.Thread(target=coin_loop, args=(k,), daemon=True) for k in SERIES]
    for t in threads:
        t.start()
    last_settle = 0
    signal.signal(signal.SIGTERM, lambda *a: halt.set())  # systemctl stop: windows exit and cancel orders
    while state["losses"] < MAX_LOSSES and not halt.is_set():
        if time.time() - last_settle > 30:
            settle_pending()
            last_settle = time.time()
        save_state()
        time.sleep(0.5)
    halt.set()
    for t in threads:
        t.join(timeout=10)
    state["status"] = f"KILL SWITCH: {state['losses']} losses" if state["losses"] >= MAX_LOSSES else "stopped"
    save_state()
    print(f"{state['status']}. Final pnl {state['pnl']:+.2f}")


def test():
    assert slug_for("btc", "5m", 1793473899) == "btc-updown-5m-1793473800"
    assert slug_for("eth", "15m", 1793474000) == "eth-updown-15m-1793473200"
    assert slug_for("btc", "1h", 1793504900) == "bitcoin-up-or-down-october-31-2026-11pm-et"
    assert pnl(0.99, 1.0, 10) == 0.1
    assert pnl(0.99, 0.90, 10) == -0.9
    assert pnl(0.95, 0.0, 10) == -9.5
    # winner: the market's last trade must price a side at >= 0.98
    assert winner(0.985) == 0 and winner(0.01) == 1
    assert winner(0.97) is None and winner(0.5) is None and winner(None) is None
    assert can_rest(1.0) and not can_rest(0.99)
    # queue: 100 ahead, a 60 sell leaves 40 ahead; a 50 sell then fills 10 of ours
    assert sim_fill(0.99, 100, [(0.99, 60)], 500) == (40, 0)
    assert sim_fill(0.99, 40, [(0.99, 50)], 500) == (0, 10)
    assert sim_fill(0.99, 100, [(0.98, 1)], 500)[1] == float("inf")
    assert sim_fill(0.99, 100, [], 30) == (30, 0)   # cancels shrink the queue
    assert sim_fill(0.99, 100, [(1.0, 999)], 500) == (100, 0)  # sells above our price don't touch us
    # a buy of the other token at 0.01 reaches our 0.99 bid
    f = Feed.__new__(Feed)
    f.books, f.sells, f.lock, f.up, f.p_up, f.last = {"U": ({}, {}), "D": ({}, {})}, {"U": [], "D": []}, threading.Lock(), "U", None, 0
    f.other = {"U": "D", "D": "U"}
    f.on_message(None, json.dumps({"event_type": "last_trade_price", "asset_id": "U", "price": "0.01", "size": "7", "side": "BUY"}))
    assert f.sells["D"] == [(0.99, 7.0)] and f.p_up == 0.01 and winner(f.p_up) == 1
    print("ok")


if __name__ == "__main__":
    if "--test" in sys.argv:
        test()
    else:
        try:
            run()
        except KeyboardInterrupt:
            halt.set()
            state["status"] = "stopped (Ctrl-C)"
            save_state()
            print(f"stopped. pnl {state['pnl']:+.2f}")
