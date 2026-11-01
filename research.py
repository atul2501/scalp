"""Backtest the scalp bot's 0.99 maker rules on Polygemy's historical Up/Down markets (read-only).

Usage: <polygemy>/python/.venv/bin/python research.py [history_dir]

Fills come only from real taker sells of our side at our price after we join (queue-ahead Q
must be eaten first) or from any sell through our price. Times are shifted -2 s for the
data-api lag. Thresholds are chosen on data before SPLIT and reported on data after it.
"""
import glob, json, math, sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else
            "/Users/atul2501/Desktop/atul/polygemy/polygemy/data/history")
SHIFT = 2           # data-api timestamps are ~2.1 s late
SIZE = 10           # shares per fill
GRACE = 90          # seconds after the window end that a bid may still fill
SPLIT = pd.Timestamp("2026-10-01", tz="UTC")
STOPS = [None, 0.30, 0.50, 0.70, 0.90]
QUEUES = [0, 500, 2000, 5000]
OUT = Path(__file__).with_name("research_out")


def wilson_lb(w, n, z=1.96):
    if n == 0:
        return float("nan")
    p = w / n
    return (p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)


def load_binance():
    b = pd.concat(pd.read_parquet(f) for f in sorted(glob.glob(str(ROOT / "binance" / "*.parquet"))))
    b = b.drop_duplicates("ts").sort_values("ts")
    t = b.ts.astype("int64").to_numpy() // 10**9
    lc = np.log(b.close.to_numpy())
    sig = pd.Series(np.r_[np.nan, np.diff(lc)]).rolling(300, min_periods=120).std().to_numpy()
    return t, lc, sig


def at(bt, x, secs):
    """Value of series x at each second (last value at or before it)."""
    i = np.searchsorted(bt, secs, side="right") - 1
    return np.where(i >= 0, x[np.maximum(i, 0)], np.nan)


def side_sells(pu, flow, up, level):
    """Masks of taker sells of our side exactly at `level` and strictly through it."""
    sp = pu if up else 1 - pu
    sell = flow < 0 if up else flow > 0
    eq = sell & (np.abs(sp - level) < 1e-6)
    thru = sell & (sp < level - 1e-6)
    return sp, eq, thru


def fill_time(tt, size, eq, thru, join, deadline, queue):
    """First second our bid fills after `join` (exclusive) up to `deadline`, or None."""
    w = (tt > join) & (tt <= deadline)
    cands = []
    e = np.flatnonzero(w & eq)
    if len(e):
        cum = np.cumsum(size[e])
        k = np.searchsorted(cum, queue, side="right")
        if k < len(e):
            cands.append(tt[e[k]])
    t = np.flatnonzero(w & thru)
    if len(t):
        cands.append(tt[t[0]])
    return min(cands) if cands else None


def outcome_pnls(tt, sp, f, end, level, win):
    """P&L per stop level for a fill at time f, holding to settlement unless stopped before the end."""
    after = (tt > f) & (tt < end)
    res = {}
    for s in STOPS:
        hit = np.flatnonzero(after & (sp <= s)) if s is not None else []
        if len(hit):
            res[s] = ((sp[hit[0]] - level) * SIZE, "stop")
        else:
            res[s] = (((1.0 if win else 0.0) - level) * SIZE, "win" if win else "loss")
    return res


def simulate(m, tr, bt, lc, sig):
    s0, end = int(m.start.timestamp()), int(m.end.timestamp())
    tt = tr.ts.astype("int64").to_numpy() // 10**9 - SHIFT
    pu, size, flow = tr.p_up.to_numpy(), tr["size"].to_numpy(), tr.up_flow.to_numpy()
    secs = np.arange(s0, end)
    l0 = at(bt, lc, np.array([s0]))[0]
    lt, st = at(bt, lc, secs), at(bt, sig, secs)
    left = (end - secs).astype(float)
    z_up = (lt - l0) / (st * np.sqrt(left))
    l_end = at(bt, lc, np.array([end]))[0]
    d_end = 1e4 * (l_end - l0)                      # Binance move over the window, bps
    base = {"cid": m.condition_id, "day": m.start.floor("D"), "hour": m.start.hour,
            "test": m.start >= SPLIT, "d_end": d_end}
    rows = []

    def record(rule, up, level, f, queue, extra=None):
        sp, _, _ = side_sells(pu, flow, up, level)
        win = (m.outcome == "Up") == up
        r = dict(base, rule=rule, side="Up" if up else "Down", level=level, queue=queue,
                 left=end - f, win=win, sig=float(at(bt, sig, np.array([f]))[0]), **(extra or {}))
        for s, (p, why) in outcome_pnls(tt, sp, f, end, level, win).items():
            r[f"pnl_{s}"], r[f"res_{s}"] = p, why
        rows.append(r)

    for queue in QUEUES:
        # Current bot: join the first side whose price trades >= one tick below the level, bid the level.
        for level in (0.97, 0.98, 0.99):
            joins = []
            for up in (True, False):
                sp, eq, thru = side_sells(pu, flow, up, level)
                trig = np.flatnonzero((sp >= level - 0.01 - 1e-6) & (tt < end))
                if len(trig):
                    joins.append((tt[trig[0]], up, eq, thru))
            if joins:
                j, up, eq, thru = min(joins, key=lambda x: x[0])
                f = fill_time(tt, size, eq, thru, j, end, queue)
                if f is not None:
                    record(f"early@{level}", up, level, f, queue)

        # Binance signal: join when |z| >= k on that side, cancel if it falls below kc before the fill.
        for k in (1.0, 2.0, 3.0, 4.0):
            for kc in (None, 0.0, 1.0):
                ok = np.isfinite(z_up)
                hit = np.flatnonzero(ok & (np.abs(z_up) >= k))
                if not len(hit):
                    continue
                j_i = hit[0]
                up = z_up[j_i] > 0
                _, eq, thru = side_sells(pu, flow, up, 0.99)
                f = fill_time(tt, size, eq, thru, secs[j_i], end, queue)
                if f is None:
                    continue
                if kc is not None:
                    zs = z_up if up else -z_up
                    w = (secs > secs[j_i]) & (secs < f) & ok
                    if (zs[w] < kc).any():
                        continue
                record(f"z{k:g}" + (f"/c{kc:g}" if kc is not None else ""), up, 0.99, f, queue)

        # After the end: bid 0.99 on the side Binance says won, if it moved at least `thr` bps.
        for thr in (0, 2, 5, 10, 20):
            if not np.isfinite(d_end) or abs(d_end) < thr:
                continue
            up = d_end > 0
            _, eq, thru = side_sells(pu, flow, up, 0.99)
            f = fill_time(tt, size, eq, thru, end, end + GRACE, queue)
            if f is not None:
                record(f"after_end>={thr}bps", up, 0.99, f, queue)

        # After the end, confirmed by the market itself: the last trade before the end puts our side
        # at >= px (optionally Binance must agree). Polymarket settles on Chainlink, which the market sees.
        pre = np.flatnonzero(tt < end)
        if len(pre):
            p_end = pu[pre[-1]]
            up = p_end >= 0.5
            sp_end = p_end if up else 1 - p_end
            for px in (0.90, 0.95, 0.98):
                for with_bn in (False, True):
                    if sp_end < px or (with_bn and not (np.isfinite(d_end) and (d_end > 0) == up)):
                        continue
                    _, eq, thru = side_sells(pu, flow, up, 0.99)
                    f = fill_time(tt, size, eq, thru, end, end + GRACE, queue)
                    if f is not None:
                        record(f"after_end_mkt>={px}" + ("+bn" if with_bn else ""), up, 0.99, f, queue)

        # Late join: post X s before the end if the market already prices a side >= 0.98, for queue
        # priority; risks a fill before the end. Kept through the grace period.
        for x in (2, 5, 10, 20, 30, 60):
            pre = np.flatnonzero(tt < end - x)
            if not len(pre):
                continue
            p_x = pu[pre[-1]]
            up = p_x >= 0.5
            if (p_x if up else 1 - p_x) < 0.98:
                continue
            _, eq, thru = side_sells(pu, flow, up, 0.99)
            f = fill_time(tt, size, eq, thru, end - x, end + GRACE, queue)
            if f is not None:
                record(f"late_mkt>=0.98@-{x}s", up, 0.99, f, queue)
    return rows


def summarize(df, stop):
    g = df.groupby(["rule", "queue", "test"])
    out = []
    for (rule, q, test), d in g:
        res = d[f"res_{stop}"]
        n, w = len(d), int((res == "win").sum())
        days = d.day.nunique()
        out.append({"rule": rule, "queue": q, "set": "test" if test else "train", "fills": n,
                    "wins": w, "stops": int((res == "stop").sum()), "losses": int((res == "loss").sum()),
                    "win%": round(100 * w / n, 2), "win_lb%": round(100 * wilson_lb(w, n), 2),
                    "pnl": round(d[f"pnl_{stop}"].sum(), 2), "pnl/day": round(d[f"pnl_{stop}"].sum() / max(days, 1), 2),
                    "worst_day": round(d.groupby("day")[f"pnl_{stop}"].sum().min(), 2)})
    return pd.DataFrame(out)


def main():
    meta = json.loads((ROOT / "meta.json").read_text()) if (ROOT / "meta.json").exists() else {"series": "btc-5m"}
    markets = pd.read_parquet(ROOT / "markets.parquet").dropna(subset=["outcome"])
    bt, lc, sig = load_binance()
    rows, used, markets_by_set, proxy = [], 0, {True: 0, False: 0}, []
    for m in markets.itertuples():
        f = ROOT / "trades" / f"{m.condition_id}.parquet"
        if not f.exists():
            continue
        tr = pd.read_parquet(f)
        if tr.empty:
            continue
        used += 1
        markets_by_set[m.start >= SPLIT] += 1
        s0, end = int(m.start.timestamp()), int(m.end.timestamp())
        l0, le = at(bt, lc, np.array([s0, end]))
        proxy.append({"d_end": 1e4 * (le - l0), "ptb_gap": 1e4 * (l0 - math.log(m.price_to_beat)),
                      "agree": ((le > l0) == (m.outcome == "Up"))})
        rows += simulate(m, tr, bt, lc, sig)
    df = pd.DataFrame(rows)
    OUT.mkdir(exist_ok=True)
    tag = meta.get("series", ROOT.name)
    df.to_parquet(OUT / f"fills_{tag}.parquet")

    pd.set_option("display.width", 200, "display.max_rows", 500)
    print(f"series {tag}: {used} markets with trades (train {markets_by_set[False]}, test {markets_by_set[True]})\n")

    p = pd.DataFrame(proxy)
    p["absd"] = pd.cut(p.d_end.abs(), [0, 1, 2, 5, 10, 20, np.inf], right=False)
    print("Proxy check: Binance window move vs official outcome")
    print(p.groupby("absd", observed=True).agg(markets=("agree", "size"), agree_pct=("agree", lambda x: round(100 * x.mean(), 2))))
    print(f"Binance at start minus official price-to-beat (bps): median {p.ptb_gap.median():.2f}, "
          f"p5 {p.ptb_gap.quantile(.05):.2f}, p95 {p.ptb_gap.quantile(.95):.2f}\n")

    for stop in STOPS:
        s = summarize(df, stop)
        s["fills/mkt%"] = [round(100 * r.fills / markets_by_set[r.set == "test"], 1) for r in s.itertuples()]
        s.to_csv(OUT / f"summary_{tag}_stop{stop}.csv", index=False)
        if stop in (None, 0.30, 0.90):
            print(f"=== stop {stop} (test set = out-of-sample Oct) ===")
            print(s[s.set == "test"].sort_values(["queue", "pnl"], ascending=[True, False]).to_string(index=False))
            print()

    base = df[(df.rule == "early@0.99") & (df.queue == 0)]
    if len(base):
        base = base.assign(vol_decile=pd.qcut(base.sig.rank(method="first"), 10, labels=False))
        print("early@0.99, Q=0, no stop: losses by volatility decile and fill time")
        print(base.groupby("vol_decile").agg(fills=("win", "size"), loss_pct=("win", lambda x: round(100 * (1 - x.mean()), 2))).T)
        lb = pd.cut(base.left, [-GRACE, 0, 30, 60, 120, 300, 900, 3600], right=True)
        print(base.groupby(lb, observed=True).agg(fills=("win", "size"), loss_pct=("win", lambda x: round(100 * (1 - x.mean()), 2))).T)


if __name__ == "__main__":
    main()
