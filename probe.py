"""Read-only probe: at 2 s before each 5-min window ends, snapshot the winner side's bid levels >= 0.98
(queue per price) and count the shares sold into each level until 90 s after the end.
Usage: python probe.py [rounds]"""
import sys, threading, time
from collections import defaultdict

import bot

ROUNDS = int(sys.argv[1]) if len(sys.argv) > 1 else 2
out_lock = threading.Lock()


def watch(coin, m):
    feed = bot.Feed(m["tokens"])
    while time.time() < m["end"] - bot.LEAD:
        time.sleep(0.05)
    i = bot.winner(feed.p_up)
    if i is None:
        feed.close()
        with out_lock:
            print(f"{coin}: undecided at -2s (Up price {feed.p_up})")
        return
    tok = m["tokens"][i]
    with feed.lock:
        bids = {p: s for p, s in feed.books[tok][0].items() if p >= 0.98}
        ask = min(feed.books[tok][1], default=1.0)
    tick = bot.get(f"{bot.CLOB}/book", token_id=tok).get("tick_size")
    feed.take_sells(tok)
    sold = defaultdict(float)
    while time.time() < m["end"] + bot.GRACE:
        for p, s in feed.take_sells(tok):
            sold[round(p, 3)] += s
        time.sleep(0.2)
    feed.close()
    with out_lock:
        print(f"\n{coin} {m['outcomes'][i]}  tick {tick}  best ask {ask}")
        print("  price   queued@-2s   sold after")
        for p in sorted(set(bids) | set(sold), reverse=True):
            print(f"  {p:<7} {bids.get(p, 0):>11,.0f} {sold.get(p, 0):>12,.0f}")


for r in range(ROUNDS):
    ms = {c: bot.get_market(bot.slug_for(c, "5m", time.time())) for c in bot.COINS}
    end = next(iter(ms.values()))["end"]
    print(f"\n######## round {r + 1}: window ends {time.strftime('%H:%M:%S', time.gmtime(end))}Z")
    ts = [threading.Thread(target=watch, args=(c, m)) for c, m in ms.items() if m]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    time.sleep(max(0, end + bot.GRACE + 2 - time.time()))
