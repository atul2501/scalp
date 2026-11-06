"""Polymarket crypto Up/Down: maker-bid 0.99 on the near-certain side, hold to the end, quit after 5 losses or -$20.

LIVE=0 (default) is dry-run: public endpoints only, simulated fills with a live-like queue model
(order latency, fair-share cancels, fills from the other token, a money limit). LIVE=1 + keys in .env = real orders.
One thread per coin/window series; dashboard on PORT while the bot runs.
"""
import csv, json, os, signal, sys, threading, time, traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import requests
import websocket
from dotenv import load_dotenv

load_dotenv()
ALL_SPANS = {"5m": 300, "15m": 900, "1h": 3600, "4h": 14400}  # every coin trades each of these windows
# hype also exists but its book is too thin
COINS = os.getenv("COINS", "btc,eth,sol,doge,xrp,bnb").replace(" ", ",").split(",")
COINS = [c for c in COINS if c]
SPANS = {s: ALL_SPANS[s] for s in os.getenv("SPANS", "5m,15m,1h,4h").replace(" ", ",").split(",") if s}
SERIES = [f"{c}-{s}" for s in SPANS for c in COINS]   # e.g. "btc-5m", one thread each
HOURLY_NAME = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "doge": "dogecoin", "xrp": "xrp", "bnb": "bnb"}
ENTRY_MIN = float(os.getenv("MIN_PRICE", "0.98"))  # side's best bid must be at least this before we join...
ENTRY_MAX = float(os.getenv("BID", "0.99"))  # ...and we always bid exactly this (maker, front of the queue if early)
ENTRY_WINDOW = 3600  # enter any time in the last N seconds; >= window length = join as early as possible
STOP = None         # hold to the end: every stop so far (0.90 and 0.30) sold a side that went on to win
STEADY = float(os.getenv("STEADY", "20"))  # side must hold >= ENTRY_MIN this many seconds before we bid (late jumps = close markets)
NO_NEW_BIDS = 10    # no new bids in the last N seconds (prices swing, fills are worst)
SIZE = int(os.getenv("SIZE", "10"))  # shares (market min is 5)
BALANCE = float(os.getenv("BALANCE", "100"))  # dry-run play money; live reads the real USDC balance
LATENCY = float(os.getenv("LATENCY", "0.07"))  # dry-run order delay: ~65 ms measured from Stockholm, ~38 ms from Ireland
MAX_SAME_WAY = 3    # at most this many open fills needing the same direction (Up or Down) at once
MAX_LOSSES = int(os.getenv("MAX_LOSSES", "5"))  # 0 = no loss-count limit, only MAX_LOSS_USD
MAX_LOSS_USD = 20   # also stop once P&L since start reaches -$20
MIN_Z = float(os.getenv("MIN_Z", "0"))  # >0: skip unless the coin's real price is this many std-devs from flipping (0 = off)
TAKE_MIN, TAKE_MAX = (float(x) for x in os.getenv("TAKE", "0-0").split("-"))  # e.g. 0.96-0.98: buy the ask in this range at once (dry run only)
TAKER_FEE = 0.07    # Polymarket crypto Up/Down: takers pay 0.07 * p * (1-p) per share, makers nothing (checked 2026-11-02)
MIN_MOVE = float(os.getenv("MIN_MOVE", "0"))  # >0: also skip unless the coin is at least this % past its window open, our way
PULL = float(os.getenv("PULL", "0"))  # >0: cancel a waiting bid once the coin moves this % against us within PULL_SECS (0 = off)
PULL_SECS = 3
POLL = 0.2          # max wait between checks; the loop wakes on every websocket update anyway
PORT = int(os.getenv("PORT", "8000"))
DASH_HOST = os.getenv("DASH_HOST", "127.0.0.1")  # 0.0.0.0 to reach the dashboard from outside
LIVE = os.getenv("LIVE", "0") == "1"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE = "https://api.binance.com"
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

client = None
if LIVE:
    from py_clob_client_v2 import ClobClient, OrderArgs, MarketOrderArgs, OrderType, BalanceAllowanceParams, AssetType, OrderPayload, OrderMarketCancelParams
    from py_clob_client_v2.order_builder.constants import BUY, SELL
    client = ClobClient(CLOB, key=os.environ["PRIVATE_KEY"], chain_id=137,
                        signature_type=int(os.getenv("SIGNATURE_TYPE", "3")), funder=os.getenv("FUNDER"))
    client.set_api_creds(client.create_or_derive_api_key())

state = {"mode": "LIVE" if LIVE else "DRY-RUN", "started": datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "config": {"entry": f"{ENTRY_MIN}-{ENTRY_MAX}", "window": ENTRY_WINDOW, "stop": STOP, "steady": STEADY, "size": SIZE,
                    "max_losses": MAX_LOSSES, "max_loss_usd": MAX_LOSS_USD, "latency": LATENCY, "min_z": MIN_Z, "min_move": MIN_MOVE, "pull": PULL, "take": f"{TAKE_MIN}-{TAKE_MAX}" if TAKE_MAX else None},
         "wins": 0, "losses": 0, "pnl": 0.0, "balance": BALANCE, "windows": 0, "bids": 0, "fills": 0, "net_errors": 0, "errors": [],
         "coins": {k: {"market": None, "position": None} for k in SERIES}, "pending": [], "status": "starting"}
lock = threading.Lock()     # guards counters, pending, trades.csv and status.json
halt = threading.Event()    # kill switch: coin threads finish their window and exit


def bump(key, n=1):
    with lock:
        state[key] = round(state[key] + n, 4)


def error(where, what):
    """Count a problem, print it, and keep the last 100 for the dashboard's error log."""
    print(f"  ERROR {where}: {what}")
    with lock:
        state["net_errors"] += 1
        state["errors"] = (state["errors"] + [{"time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                               "where": where, "what": what}])[-100:]


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
    start = int(ts) // ALL_SPANS[span] * ALL_SPANS[span]
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
    if left > ENTRY_WINDOW or left < NO_NEW_BIDS or bid < ENTRY_MIN:
        return None
    p = ENTRY_MAX
    return p if ask > p else None  # ask above us, so the bid rests = maker


def steady(since, now):
    """True once the side's bid has stayed >= ENTRY_MIN for STEADY seconds."""
    return since is not None and now - since >= STEADY


def sim_fill(price, ahead, sells, level, prev):
    """Dry-run queue model. Taker sells at our price eat the queue ahead of us first;
    a sell below our price means our whole level was cleared. `prev` and `level` are the
    level size (without us) before and after; any drop not explained by trades is cancels,
    spread evenly in front of and behind us. Returns (ahead, filled)."""
    filled = traded = 0.0
    for p, s in sells:
        if p < price:
            return 0.0, float("inf")
        if p == price:
            eat = min(ahead, s)
            ahead -= eat
            filled += s - eat
            traded += s
    rest = prev - traded  # level size if nobody had cancelled
    if 0 < level < rest:
        ahead *= level / rest
    return min(ahead, level), filled


def z_score(open_, closes, up, left):
    """(z, move): how many std-devs the coin is from flipping the window, on our side (+) or not (-),
    given its per-second volatility in `closes` and `left` seconds to go; move = % past the open, our way."""
    rets = [(b - a) / a for a, b in zip(closes, closes[1:])]
    var = sum(r * r for r in rets) / max(len(rets), 1)
    d = (closes[-1] - open_) / open_ * (1 if up else -1)
    return (d / ((var * left) ** 0.5) if var else (float("inf") if d > 0 else 0.0)), d * 100


def odds_ok(odds):
    """odds = (z, move %) from fair_z, or None if Binance couldn't be read (then don't bid)."""
    return odds is not None and odds[0] >= MIN_Z and odds[1] >= MIN_MOVE


def fair_z(coin, start, up, left):
    """(z, move) from Binance 1s candles: the open at window start, then the last 5 minutes. None on failure."""
    url, sym = f"{BINANCE}/api/v3/klines", coin.upper() + "USDT"
    try:
        first = requests.get(url, params={"symbol": sym, "interval": "1s", "startTime": int(start) * 1000, "limit": 1}, timeout=3).json()
        recent = requests.get(url, params={"symbol": sym, "interval": "1s", "limit": 300}, timeout=3).json()
        return z_score(float(first[0][1]), [float(k[4]) for k in recent], up, left)
    except (requests.RequestException, ValueError, IndexError, KeyError, TypeError) as e:
        error(f"binance {sym}", repr(e))
        return None


ticks = {}  # PULL: coin -> [(arrival time, Binance trade price)] for the last PULL_SECS
ticks_lock = threading.Lock()


def watch_binance():
    """PULL: live Binance trades for every coin, so a waiting bid can be cancelled the moment the coin jumps."""
    def on_message(ws, raw):
        x = json.loads(raw)
        coin, now = x["stream"].split("usdt@")[0], time.time()
        with ticks_lock:
            t = ticks.setdefault(coin, [])
            t.append((now, float(x["data"]["p"])))
            while t[0][0] < now - PULL_SECS:
                t.pop(0)
    url = BINANCE_WS + "/".join(f"{c}usdt@aggTrade" for c in COINS)
    while not halt.is_set():
        websocket.WebSocketApp(url, on_message=on_message).run_forever(ping_interval=10, ping_timeout=5, skip_utf8_validation=True)
        if not halt.is_set():
            error("binance ws", "feed dropped, reconnected")
            time.sleep(1)


def against(prices, up):
    """% the coin moved against our side in `prices` (oldest first): fall from the high for Up, rise from the low for Down."""
    if not prices:
        return 0.0
    hi, lo, last = max(prices), min(prices), prices[-1]
    return (hi - last) / hi * 100 if up else (last - lo) / lo * 100


def recent(coin):
    with ticks_lock:
        return [p for t, p in ticks.get(coin, []) if t >= time.time() - PULL_SECS]


def same_way(coins, side):
    """Open filled positions (still inside their window) betting on `side`."""
    return sum(1 for c in coins.values() if c["position"] and c["position"]["held"] and c["position"]["side"] == side)


def keep_running(losses, pnl_):
    return (not MAX_LOSSES or losses < MAX_LOSSES) and pnl_ > -MAX_LOSS_USD


def live_balance():
    r = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
    return int(r["balance"]) / 1e6  # pUSD has 6 decimals


def reserve(cost):
    """Dry-run: lock money for a resting bid, like the exchange does. Live: just check the last known balance."""
    with lock:
        if state["balance"] < cost:
            return False
        if not LIVE:
            state["balance"] = round(state["balance"] - cost, 4)
        return True


def get(url, **params):
    """GET json, retrying forever so a held position is never abandoned on a network blip."""
    attempt = 0
    while True:
        try:
            return requests.get(url, params=params, timeout=5).json()
        except (requests.RequestException, ValueError) as e:
            attempt += 1
            error(url.split("/")[2], f"{type(e).__name__}, retry {attempt}")
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

    def __init__(self, tokens, label=""):
        self.label = label
        self.books = {t: ({}, {}) for t in tokens}
        self.sells = {t: [] for t in tokens}
        self.other = {tokens[0]: tokens[1], tokens[1]: tokens[0]}
        self.changed = threading.Event()  # set on every update so the trading loop reacts at once
        self.last = 0.0
        self.alive = True
        self.lock = threading.Lock()
        self.ws = websocket.WebSocketApp(
            WS, on_open=lambda ws: ws.send(json.dumps({"assets_ids": tokens, "type": "market"})),
            on_message=self.on_message)
        threading.Thread(target=self.run, daemon=True).start()

    def run(self):
        while self.alive:
            self.ws.run_forever(ping_interval=10, ping_timeout=5, skip_utf8_validation=True)  # pure-Python utf8 check was half our CPU
            if self.alive:  # Polymarket dropped the feed: say so and reconnect quickly (books stay as last seen)
                error(self.label, "price feed dropped, reconnected")
                time.sleep(0.3)

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
                    if x.get("side") == "SELL":
                        self.sells[tok].append((p, float(x["size"])))
                    else:  # buying one token at p also fills bids on the other at 1 - p (the exchange mints a pair)
                        self.sells[self.other[tok]].append((round(1 - p, 4), float(x["size"])))
        self.changed.set()

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


def log_skip(slug, side, left, why):
    with lock:
        new = not os.path.exists("skipped.csv")
        with open("skipped.csv", "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["time", "slug", "side", "left_when_biddable", "why"])
            w.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"), slug, side, left, why])


def settle_pending():
    """Check held positions from finished windows; Gamma marks the winner some minutes after close."""
    for p in list(state["pending"]):
        r = get_market(p["slug"], closed=True)
        if r and r["prices"] and float(r["prices"][p["idx"]]) in (0.0, 1.0):
            payout = float(r["prices"][p["idx"]])
            closed_at = datetime.fromtimestamp(p["end"], timezone.utc)
            log(p["slug"], p["side"], p["entry"], payout, p["held"], "win" if payout == 1.0 else "loss", closed_at)
            if not LIVE:
                bump("balance", payout * p["held"])  # as if claimed right away
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
    feed = Feed(m["tokens"], m["slug"])
    order_id, side_idx, entry, held, ahead, prev = None, None, None, 0.0, None, 0.0
    stopped = False
    since = [None] * len(m["tokens"])  # when each side's bid last reached ENTRY_MIN
    lands = checked = 0.0  # dry-run: when our order reaches the book; live: last fill check
    skipped = [None] * len(m["tokens"])  # [seconds left when first biddable, why not bid] per side
    signed = [None] * len(m["tokens"])   # live: order signed ahead of time, sent the moment the rules allow
    z = [None] * len(m["tokens"])        # MIN_Z/MIN_MOVE: (z, move %) per side, fetched just before the bid is allowed
    name, span = coin.split("-")
    take_px, checked_at = None, [0.0] * len(m["tokens"])  # TAKE: ask we bought at; last odds check per side
    pull_at = None  # PULL: when our cancel reaches the book (fills until then still count)
    if LIVE:
        for t in m["tokens"]:  # cached by the client, so posting the order needs no extra requests
            client.get_tick_size(t)
            client.get_neg_risk(t)
    try:
        while time.time() < m["end"] and not halt.is_set():
            feed.changed.clear()
            books = [feed.top(t) for t in m["tokens"]]
            left = int(m["end"] - time.time())
            view["market"] = {"slug": m["slug"], "outcomes": m["outcomes"], "books": books, "seconds_left": left}

            if side_idx is None and TAKE_MAX:  # taker test: Polymarket lags Binance, buy the cheap ask while it lasts
                now = time.time()
                for i, (bid, ask) in enumerate(books):
                    if not TAKE_MIN <= ask <= TAKE_MAX or not NO_NEW_BIDS <= left <= ENTRY_WINDOW or now - checked_at[i] < 2:
                        continue
                    checked_at[i] = now
                    skipped[i] = skipped[i] or [left, "odds"]
                    odds = fair_z(name, m["end"] - ALL_SPANS[span], m["outcomes"][i] == "Up", left)
                    if not odds_ok(odds):
                        continue
                    if same_way(state["coins"], m["outcomes"][i]) >= MAX_SAME_WAY:
                        skipped[i][1] = "same-way cap"
                        continue
                    entry = round(ask + TAKER_FEE * ask * (1 - ask), 4)  # fee folded into the price we pay
                    if not reserve(entry * SIZE):
                        skipped[i][1] = "low balance"
                        continue
                    side_idx, tok, take_px, lands = i, m["tokens"][i], ask, now + LATENCY
                    bump("bids")
                    view["position"] = {"side": m["outcomes"][i], "status": "taking", "held": 0, "entry": entry, "ahead": 0}
                    print(f"  {coin} TAKE {m['outcomes'][i]} {SIZE}@{ask} (+fee = {entry})  {left}s left  z={odds[0]:.2f} move={odds[1]:.3f}%")
                    break

            elif side_idx is None:
                now = time.time()
                for i, (bid, ask) in enumerate(books):
                    since[i] = (since[i] or now) if bid >= ENTRY_MIN else None
                    if since[i] is None:
                        z[i] = None  # fell back below 0.98: re-check the odds if it comes back
                    entry = entry_price(bid, ask, left)
                    if entry is None:
                        continue
                    skipped[i] = skipped[i] or [left, "unsteady"]
                    if not steady(since[i], now):
                        if steady(since[i], now + 3):
                            if LIVE and signed[i] is None:
                                signed[i] = client.create_order(OrderArgs(token_id=m["tokens"][i], price=entry, size=SIZE, side=BUY))
                            if (MIN_Z or MIN_MOVE) and z[i] is None:
                                z[i] = fair_z(name, m["end"] - ALL_SPANS[span], m["outcomes"][i] == "Up", left)
                        continue
                    if (MIN_Z or MIN_MOVE) and not odds_ok(z[i]):
                        skipped[i][1] = "odds"
                        continue  # Polymarket says near-certain, the coin's real price says it could still flip
                    if same_way(state["coins"], m["outcomes"][i]) >= MAX_SAME_WAY:
                        skipped[i][1] = "same-way cap"
                        continue  # too many open bets on one direction already
                    if not reserve(entry * SIZE):
                        skipped[i][1] = "low balance"
                        if not view.get("low"):
                            print(f"  {coin} low balance ({state['balance']:.2f}), skipping bid")
                        view["low"] = True
                        continue
                    view["low"] = False
                    side_idx, tok = i, m["tokens"][i]
                    lands = now + LATENCY
                    bump("bids")
                    view["position"] = {"side": m["outcomes"][i], "status": "bidding", "held": 0,
                                         "entry": entry, "ahead": round(feed.level(tok, entry), 2)}
                    print(f"  {coin} BID {m['outcomes'][i]} {SIZE}@{entry}  queue ahead {feed.level(tok, entry):.0f}  {left}s left" + (f"  z={z[i][0]:.2f} move={z[i][1]:.3f}%" if MIN_Z or MIN_MOVE else ""))
                    if LIVE:
                        o = signed[i] or client.create_order(OrderArgs(token_id=tok, price=entry, size=SIZE, side=BUY))
                        resp = client.post_order(o, OrderType.GTC, post_only=True)
                        order_id = resp.get("orderID")
                        if not order_id:
                            error(m["slug"], f"order rejected: {resp}")
                    break

            elif held < SIZE:  # waiting on (more) fill
                tok = m["tokens"][side_idx]
                bid, ask = books[side_idx]
                was = held
                if take_px is not None:  # taker: fills on landing from the asks still at or below our price, else nothing
                    if time.time() >= lands:
                        with feed.lock:
                            held = min(SIZE, sum(sz for px, sz in feed.books[tok][1].items() if px <= take_px))
                        take_px = None
                elif LIVE and order_id and time.time() - checked >= 1:  # once a second: stay under rate limits
                    checked = time.time()
                    o = client.get_order(order_id)  # null for a moment right after posting
                    held = float(o.get("size_matched", held)) if o else held
                elif not LIVE and time.time() < lands:
                    feed.take_sells(tok)  # trades before our order lands can't fill it
                elif not LIVE:
                    lvl = feed.level(tok, entry)
                    if ahead is None:  # just landed: join the back of the queue as it is now
                        ahead, prev = lvl, lvl
                        feed.take_sells(tok)
                    ahead, filled = sim_fill(entry, ahead, feed.take_sells(tok), lvl, prev)
                    prev = lvl
                    if ask <= entry:  # a seller resting at our price means our level was cleared
                        filled = SIZE
                    held = min(SIZE, held + filled)
                    view["position"]["ahead"] = round(ahead, 2)
                if held > was:
                    if not was:
                        bump("fills")
                    view["position"].update(status="filled", held=held)
                    print(f"  {coin} FILLED {held} @ {entry}  {left}s left")
                if PULL and take_px is None and pull_at is None and held < SIZE:
                    move = against(recent(name), m["outcomes"][side_idx] == "Up")
                    if move >= PULL:  # coin jumping toward a flip: sellers are about to dump into our bid
                        pull_at = time.time() + LATENCY
                        print(f"  {coin} PULL {m['outcomes'][side_idx]} bid: coin moved {move:.3f}% against us  {left}s left  held {held}")
                        if LIVE and order_id:
                            client.cancel_order(OrderPayload(orderID=order_id))
                if pull_at and time.time() >= pull_at:
                    if LIVE and order_id:
                        o = client.get_order(order_id)
                        held = float(o.get("size_matched", held)) if o else held
                    break

            if held and STOP is not None:
                bid, _ = books[side_idx]
                if bid <= STOP:
                    if LIVE:
                        if order_id:
                            client.cancel_order(OrderPayload(orderID=order_id))
                        client.post_order(client.create_market_order(MarketOrderArgs(
                            token_id=m["tokens"][side_idx], amount=held, side=SELL)), OrderType.FAK)
                    log(m["slug"], m["outcomes"][side_idx], entry, bid, held, "stop")
                    stopped = True
                    break
            feed.changed.wait(POLL)
    finally:
        feed.close()
        if LIVE and side_idx is not None and not stopped:  # drop any unfilled remainder, even if a crash lost the order id
            client.cancel_market_orders(OrderMarketCancelParams(asset_id=m["tokens"][side_idx]))
        if side_idx is not None and not LIVE:
            bump("balance", entry * (SIZE - held))  # unfilled part of the bid frees its money

    for i, sk in enumerate(skipped):  # could have bid but didn't: check later whether that side won
        if sk and i != side_idx and not halt.is_set():
            log_skip(m["slug"], m["outcomes"][i], *sk)
    if pull_at and not held and not halt.is_set():
        log_skip(m["slug"], m["outcomes"][side_idx], left, "pulled")  # settles later as would-have-won/lost: the cost of pulling
    elif side_idx is not None and not held and not halt.is_set():
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
        m = None
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
            error(key, repr(e))
            traceback.print_exc()
            # sit out the rest of a window that failed mid-trade: retrying it could bid twice in one market
            halt.wait(max(5, m["end"] - time.time() + 1) if m else 5)


def run():
    print(f"mode={state['mode']} series={len(SERIES)} ({COINS} x {list(SPANS)}) entry={ENTRY_MIN}-{ENTRY_MAX} last {ENTRY_WINDOW}s stop={STOP} "
          f"steady={STEADY} size={SIZE} latency={LATENCY} max_losses={MAX_LOSSES} max_loss_usd={MAX_LOSS_USD} min_z={MIN_Z} min_move={MIN_MOVE} pull={PULL} take={TAKE_MIN}-{TAKE_MAX}")
    try:
        geo = requests.get("https://polymarket.com/api/geoblock", timeout=5).json()
        print(f"geoblock {'BLOCKED' if geo.get('blocked') else 'ok'} ({geo.get('country')})")
        if geo.get("blocked") and LIVE:
            sys.exit("this server's country is blocked by Polymarket; not trading live")
    except (requests.RequestException, ValueError) as e:
        print(f"geoblock check failed: {e!r}")
    if LIVE and TAKE_MAX:
        sys.exit("TAKE is a dry-run test only; it has no live order path yet")
    serve_dashboard()
    try:  # positions still waiting for a result (and the dry-run money tied up in them) survive restarts
        saved = json.load(open("status.json"))
        state["pending"] = saved.get("pending", [])
        if not LIVE:
            state["balance"] = saved.get("balance", BALANCE)
        for k in ("started", "wins", "losses", "pnl", "windows", "bids", "fills", "net_errors", "errors"):  # totals carry on
            state[k] = saved.get(k, state[k])
        print(f"restored {len(state['pending'])} pending positions")
    except (OSError, ValueError):
        pass
    if LIVE:
        state["balance"] = live_balance()
    print(f"balance {state['balance']:.2f} USDC" + (" (0 in live: check SIGNATURE_TYPE and FUNDER in .env)" if LIVE and not state["balance"] else ""))
    signal.signal(signal.SIGTERM, lambda *a: halt.set())  # systemctl stop: windows exit and cancel their orders
    if PULL:
        threading.Thread(target=watch_binance, daemon=True).start()
    state["status"] = "running"
    threads = [threading.Thread(target=coin_loop, args=(k,), daemon=True) for k in SERIES]
    for t in threads:
        t.start()
    last_settle = 0
    while keep_running(state["losses"], state["pnl"]) and not halt.is_set():
        if time.time() - last_settle > 30:
            settle_pending()
            if LIVE:
                state["balance"] = live_balance()
            last_settle = time.time()
        save_state()
        time.sleep(0.5)
    stopped_by_us = halt.is_set()
    halt.set()
    for t in threads:
        t.join(timeout=10)
    if LIVE:
        client.cancel_all()  # nothing of ours may stay on the book
    state["status"] = "stopped" if stopped_by_us else f"KILL SWITCH: {state['losses']} losses, pnl {state['pnl']:+.2f}"
    save_state()
    print(f"{state['status']}. Final pnl {state['pnl']:+.2f}")


def test():
    global MIN_Z, MIN_MOVE, MAX_LOSSES
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
    assert entry_price(0.99, 1.0, NO_NEW_BIDS - 1) is None   # too late for a new bid
    assert not steady(None, 100) and not steady(100, 100 + STEADY - 1) and steady(100, 100 + STEADY)
    # queue: 100 ahead, a 60 sell leaves 40 ahead; a 50 sell then fills 10 of ours
    assert sim_fill(0.99, 100, [(0.99, 60)], 440, 500) == (40, 0)
    assert sim_fill(0.99, 40, [(0.99, 50)], 440, 490) == (0, 10)
    assert sim_fill(0.99, 100, [(0.98, 1)], 500, 500)[1] == float("inf")
    assert sim_fill(0.99, 100, [], 250, 500) == (50, 0)    # half the level cancelled: half of those were ahead of us
    assert sim_fill(0.99, 100, [], 900, 500) == (100, 0)   # people joining behind us change nothing
    assert sim_fill(0.99, 100, [(1.0, 999)], 500, 500) == (100, 0)  # sells above our price don't touch us
    # a buy of the other token at 0.01 is a 0.99 sell into our bid
    f = Feed.__new__(Feed)
    f.books, f.sells, f.other = {"U": ({}, {}), "D": ({}, {})}, {"U": [], "D": []}, {"U": "D", "D": "U"}
    f.lock, f.changed, f.last = threading.Lock(), threading.Event(), 0.0
    error("test", "x")
    assert state["errors"][-1]["what"] == "x" and state["net_errors"] == 1
    state["errors"], state["net_errors"] = [], 0
    f.on_message(None, json.dumps({"event_type": "last_trade_price", "asset_id": "D", "price": "0.01", "size": "7", "side": "BUY"}))
    assert f.sells["U"] == [(0.99, 7.0)] and f.changed.is_set()
    # at most MAX_SAME_WAY open fills on one direction
    coins = {k: {"position": {"side": "Down", "held": 10}} for k in "abc"} | {"d": {"position": None}}
    assert same_way(coins, "Down") == 3 and same_way(coins, "Up") == 0
    # money: a bid locks its cost; not enough money = no bid
    state["balance"] = 15.0
    assert reserve(9.9) and state["balance"] == 5.1 and not reserve(9.9)
    state["balance"] = BALANCE
    assert keep_running(0, 0) and not keep_running(MAX_LOSSES, 0) and not keep_running(1, -MAX_LOSS_USD)
    MAX_LOSSES = 0  # off: only the money limit stops it
    assert keep_running(50, 21.37) and not keep_running(50, -MAX_LOSS_USD)
    MAX_LOSSES = 5
    # fair odds: no move = 0; price moving +/-1 each second, 4 above the open with 4 s left = 2 std-devs
    assert z_score(100, [100, 100, 100], True, 60) == (0.0, 0.0)
    closes = [100 + (i % 2) for i in range(301)] + [104]
    (zu, mu), (zd, md) = z_score(100, closes, True, 4), z_score(100, closes, False, 4)
    assert 1.9 < zu < 2.1 and zd == -zu and abs(mu - 4) < 1e-9 and md == -mu
    # a quiet night: high z but only 0.02% past the open is still too close when MIN_MOVE asks for 0.05%
    MIN_Z, MIN_MOVE = 2.0, 0.05
    assert odds_ok((3.4, 0.08)) and not odds_ok((3.4, 0.02)) and not odds_ok((1.5, 0.08)) and not odds_ok(None)
    MIN_Z = MIN_MOVE = 0.0
    # pull: 2 Nov 19:27 spike, ETH 2246.56 -> 2249.01 is 0.110% against a Down bid, nothing against an Up bid
    assert abs(against([2246.53, 2246.56, 2249.01], False) - 0.110) < 0.001 and against([2246.53, 2246.56, 2249.01], True) == 0
    assert abs(against([100, 99.9], True) - 0.1) < 1e-9 and against([], True) == 0
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
