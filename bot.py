"""Polymarket 5-min crypto Up/Down: maker-bid 0.99 on the near-certain side, emergency stop, quit after 5 losses.

LIVE=0 (default) is dry-run: public endpoints only, simulated fills with a queue model.
One thread per coin; dashboard on PORT while the bot runs.
"""
import csv, json, os, sys, threading, time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket
from dotenv import load_dotenv

load_dotenv()
COINS = ["btc", "eth", "sol", "doge", "xrp", "bnb"]  # hype also exists but its book is too thin
SPANS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}  # every coin trades each of these windows
SERIES = [f"{c}-{s}" for s in SPANS for c in COINS]   # e.g. "btc-5m", one thread each
HOURLY_NAME = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "doge": "dogecoin", "xrp": "xrp", "bnb": "bnb"}
ENTRY_MIN = 0.98    # side's best bid must be at least this before we join...
ENTRY_MAX = 0.99    # ...and we always bid exactly this (maker, front of the queue if early)
ENTRY_WINDOW = 3600  # enter any time in the last N seconds; >= window length = join as early as possible
STOP = None         # hold to the end: every stop so far (0.90 and 0.30) sold a side that went on to win
STEADY = 20         # side must hold >= ENTRY_MIN this many seconds before we bid (late jumps = close markets)
SIZE = 10           # shares (market min is 5)
MAX_LOSSES = 5
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
         "config": {"entry": f"{ENTRY_MIN}-{ENTRY_MAX}", "window": ENTRY_WINDOW, "stop": STOP, "steady": STEADY, "size": SIZE,
                    "max_losses": MAX_LOSSES},
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
    if span == "4h":  # 4-hour windows start at 0/4/8/12/16/20 New York time
        et = datetime.fromtimestamp(int(ts), ZoneInfo("America/New_York"))
        start = int(et.replace(hour=et.hour // 4 * 4, minute=0, second=0, microsecond=0).timestamp())
        return f"{coin}-updown-4h-{start}"
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


def entry_price(bid, ask, left):
    """Bid ENTRY_MAX the first moment it can rest on the book; None = don't enter."""
    if left > ENTRY_WINDOW or bid < ENTRY_MIN:
        return None
    p = ENTRY_MAX
    return p if ask > p else None  # ask above us, so the bid rests = maker


def steady(since, now):
    """True once the side's bid has stayed >= ENTRY_MIN for STEADY seconds."""
    return since is not None and now - since >= STEADY


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
                elif kind == "last_trade_price" and tok in self.sells and x.get("side") == "SELL":
                    self.sells[tok].append((float(x["price"]), float(x["size"])))

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


def trade_window(coin, m):
    """Run one 5-min window for one coin. Leaves a filled-and-unstopped position in state['pending']."""
    view = state["coins"][coin]
    feed = Feed(m["tokens"])
    order_id, side_idx, entry, held, ahead = None, None, None, 0.0, 0.0
    stopped = False
    since = [None] * len(m["tokens"])  # when each side's bid last reached ENTRY_MIN
    try:
        while time.time() < m["end"] and not halt.is_set():
            books = [feed.top(t) for t in m["tokens"]]
            left = int(m["end"] - time.time())
            view["market"] = {"slug": m["slug"], "outcomes": m["outcomes"], "books": books, "seconds_left": left}

            if side_idx is None:
                now = time.time()
                for i, (bid, ask) in enumerate(books):
                    since[i] = (since[i] or now) if bid >= ENTRY_MIN else None
                    entry = entry_price(bid, ask, left)
                    if entry is None or not steady(since[i], now):
                        continue
                    side_idx, tok = i, m["tokens"][i]
                    ahead = feed.level(tok, entry)
                    feed.take_sells(tok)  # only trades after we post count toward our fill
                    bump("bids")
                    view["position"] = {"side": m["outcomes"][i], "status": "bidding", "held": 0,
                                         "entry": entry, "ahead": round(ahead, 2)}
                    print(f"  {coin} BID {m['outcomes'][i]} {SIZE}@{entry}  queue ahead {ahead:.0f}  {left}s left")
                    if LIVE:
                        o = client.create_order(OrderArgs(token_id=tok, price=entry, size=SIZE, side=BUY))
                        resp = client.post_order(o, OrderType.GTC, post_only=True)
                        order_id = resp.get("orderID")
                        if not order_id:
                            print(f"  rejected: {resp}")
                    break

            elif held < SIZE:  # waiting on (more) fill
                tok = m["tokens"][side_idx]
                bid, ask = books[side_idx]
                was = held
                if LIVE and order_id:
                    held = float(client.get_order(order_id).get("size_matched", 0))
                elif not LIVE:
                    ahead, filled = sim_fill(entry, ahead, feed.take_sells(tok), feed.level(tok, entry))
                    if ask <= entry:  # a seller resting at our price means our level was cleared
                        filled = SIZE
                    held = min(SIZE, held + filled)
                    view["position"]["ahead"] = round(ahead, 2)
                if held > was:
                    if not was:
                        bump("fills")
                    view["position"].update(status="filled", held=held)
                    print(f"  {coin} FILLED {held} @ {entry}  {left}s left")

            if held and STOP is not None:
                bid, _ = books[side_idx]
                if bid <= STOP:
                    if LIVE:
                        if order_id:
                            client.cancel(order_id)
                        client.post_order(client.create_market_order(MarketOrderArgs(
                            token_id=m["tokens"][side_idx], amount=held, side=SELL)), OrderType.FAK)
                    log(m["slug"], m["outcomes"][side_idx], entry, bid, held, "stop")
                    stopped = True
                    break
            time.sleep(POLL)
    finally:
        feed.close()
        if LIVE and order_id and not stopped:
            client.cancel(order_id)  # drop any unfilled remainder

    if side_idx is not None and not held:
        log(m["slug"], m["outcomes"][side_idx], entry, "", 0, "nofill")  # bid never filled: no trade
    if held and not stopped:
        with lock:
            state["pending"].append({"slug": m["slug"], "idx": side_idx, "side": m["outcomes"][side_idx],
                                     "held": held, "entry": entry, "end": m["end"]})
    view["position"] = None


def coin_loop(key):
    """Trade every window of one coin/span series (e.g. "btc-15m") until the kill switch trips."""
    coin, span = key.split("-")
    while not halt.is_set():
        try:
            m = get_market(slug_for(coin, span, time.time()))
            if not m or m["end"] <= time.time():
                state["coins"][key]["market"] = None  # Polymarket lists no window right now (e.g. clock-change night)
                time.sleep(5)
                continue
            bump("windows")
            print(f"== {m['slug']} ends {datetime.fromtimestamp(m['end'], timezone.utc):%H:%M:%S}Z")
            trade_window(key, m)
            halt.wait(max(0, m["end"] - time.time()) + 1)  # one entry per window
        except Exception as e:  # one coin's bad window must not kill the others
            print(f"  {coin} error: {e!r}")
            time.sleep(5)


def run():
    print(f"mode={state['mode']} series={len(SERIES)} ({COINS} x {list(SPANS)}) entry={ENTRY_MIN}-{ENTRY_MAX} last {ENTRY_WINDOW}s stop={STOP} "
          f"steady={STEADY} size={SIZE} max_losses={MAX_LOSSES}")
    serve_dashboard()
    try:  # positions still waiting for a result survive restarts
        state["pending"] = json.load(open("status.json")).get("pending", [])
        print(f"restored {len(state['pending'])} pending positions")
    except (OSError, ValueError):
        pass
    state["status"] = "running"
    threads = [threading.Thread(target=coin_loop, args=(k,), daemon=True) for k in SERIES]
    for t in threads:
        t.start()
    last_settle = 0
    while state["losses"] < MAX_LOSSES:
        if time.time() - last_settle > 30:
            settle_pending()
            last_settle = time.time()
        save_state()
        time.sleep(0.5)
    halt.set()
    for t in threads:
        t.join(timeout=10)
    state["status"] = f"KILL SWITCH: {state['losses']} losses"
    save_state()
    print(f"{state['status']}. Final pnl {state['pnl']:+.2f}")


def test():
    assert slug_for("btc", "5m", 1793473899) == "btc-updown-5m-1793473800"
    assert slug_for("eth", "15m", 1793474000) == "eth-updown-15m-1793473200"
    assert slug_for("btc", "1h", 1793504900) == "bitcoin-up-or-down-october-31-2026-11pm-et"
    assert slug_for("sol", "4h", 1793504900) == "sol-updown-4h-1793491200"   # 8PM-12AM EDT
    assert slug_for("btc", "4h", 1793530000) == "btc-updown-4h-1793523600"   # 4AM-8AM EST, after the clock change
    assert pnl(0.99, 1.0, 10) == 0.1
    assert pnl(0.99, 0.90, 10) == -0.9
    assert pnl(0.95, 0.0, 10) == -9.5
    # entry: join at 0.99 as early as it can rest (bid >= 0.98, ask above 0.99)
    assert entry_price(0.98, 1.0, 250) == 0.99
    assert entry_price(0.999, 1.0, 30) == 0.99
    assert entry_price(0.98, 0.99, 250) is None   # 0.99 bid would cross the ask = taker
    assert entry_price(0.97, 1.0, 250) is None
    assert not steady(None, 100) and not steady(100, 100 + STEADY - 1) and steady(100, 100 + STEADY)
    # queue: 100 ahead, a 60 sell leaves 40 ahead; a 50 sell then fills 10 of ours
    assert sim_fill(0.99, 100, [(0.99, 60)], 500) == (40, 0)
    assert sim_fill(0.99, 40, [(0.99, 50)], 500) == (0, 10)
    assert sim_fill(0.99, 100, [(0.98, 1)], 500)[1] == float("inf")
    assert sim_fill(0.99, 100, [], 30) == (30, 0)   # cancels shrink the queue
    assert sim_fill(0.99, 100, [(1.0, 999)], 500) == (100, 0)  # sells above our price don't touch us
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
