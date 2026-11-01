# Scalp bot

A Polymarket maker bot for the crypto "Up or Down" markets (BTC, ETH, SOL, DOGE, XRP and BNB; 5m, 15m, 1h and 4h windows).

In the last hour of a window, once one side has held at 0.98 or higher for 20 seconds, the bot places a 0.99 post-only bid on that side. A filled share pays $1.00 when its side wins, so each win makes about $0.10 on a 10-share bid. A loss costs $9.90. There is no stop-loss: every fill is held until Polymarket settles the market.

The same code runs dry (simulated fills) or live (real orders); `LIVE` in `.env` is the only switch. A dashboard is served on `PORT`, 8000 by default.

## 1. Server

- **Region:** use a region that Polymarket allows. **Ireland and the UK are blocked**; Stockholm (`eu-north-1`) works. Check any new server first:
  ```bash
  curl -s https://polymarket.com/api/geoblock    # needs "blocked":false
  ```
- **Size:** Amazon Linux 2023 on a t3.micro is enough.
- **Security group:**
  - SSH (22), your IP only.
  - TCP 8000 (dashboard), **your IP only**. The dashboard is read-only, but it shows your balance and positions.

## 2. Install

```bash
sudo dnf install -y git python3.11
git clone https://github.com/atul2501/scalp.git ~/scalp
cd ~/scalp
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python bot.py --test        # ends with "ok"
```

## 3. Configure `.env`

```bash
cp .env.example .env
chmod 600 .env
nano .env
```

**Dry run (start here):**

```
LIVE=0
DASH_HOST=0.0.0.0
BALANCE=100
```

**Live:**

1. On polymarket.com, deposit money. It is held as pUSD.
2. Make one small trade on the website. This sets up the trading approvals; the bot can't do that step itself.
3. In Settings, export the private key.
4. Copy the wallet address from your profile.
5. Set these in `.env`:

```
LIVE=1
PRIVATE_KEY=0x...        # only on the server, never in git or chat
FUNDER=0x...             # your Polymarket wallet address (profile), not the private key's own address
SIGNATURE_TYPE=3         # 3 = any account made since May 2026; 1 = older email/Google, 2 = older MetaMask
DASH_HOST=0.0.0.0
```

If the log shows `balance 0.00` in live mode, `SIGNATURE_TYPE` or `FUNDER` is wrong.

The other settings, with their defaults, are explained in `.env.example`: `COINS`, `SPANS`, `SIZE`, `STEADY`, `LATENCY`, `PORT`. `.env` is in `.gitignore`, and the dashboard never serves it.

## 4. Run as a service

```bash
sudo tee /etc/systemd/system/scalp.service >/dev/null <<'EOF'
[Unit]
Description=Polymarket scalp bot (dry-run unless LIVE=1 in .env)
After=network-online.target
Wants=network-online.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/scalp
ExecStart=/home/ec2-user/scalp/.venv/bin/python -u bot.py
Restart=on-failure
RestartSec=5
StandardOutput=append:/home/ec2-user/scalp/bot.log
StandardError=append:/home/ec2-user/scalp/bot.log

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now scalp     # starts now and after every reboot
```

| What | Command |
|---|---|
| Status | `systemctl status scalp` |
| Log | `tail -f ~/scalp/bot.log` |
| Stop (cancels open bids) | `sudo systemctl stop scalp` |
| Restart (after editing `.env`) | `sudo systemctl restart scalp` |
| Dashboard | `http://<server-ip>:8000/` |

## 5. Switch from dry run to live

The bot keeps its totals and open positions in `status.json` across restarts. Move the dry-run files aside so they don't mix with real money and so the kill switch starts from zero:

```bash
cd ~/scalp
sudo systemctl stop scalp
mkdir -p dryrun && mv status.json trades.csv skipped.csv bot.log dryrun/ 2>/dev/null
nano .env                    # LIVE=1, PRIVATE_KEY, FUNDER, SIGNATURE_TYPE
sudo systemctl start scalp
head -5 bot.log              # mode=LIVE, "geoblock ok (SE)", "balance <your pUSD>"
```

If the log says the country is blocked, the bot exits without trading.

**Start small:** for the first day, set `COINS=btc`, `SPANS=5m,15m` and `SIZE=5` in `.env`. When the results match the dry run, remove those lines and restart.

## Safety

- **Kill switch:** the bot stops itself after **5 losses** or **−$20**. To stop it immediately, run `sudo systemctl stop scalp`.
- **Never** commit `.env` or paste the private key anywhere.
- **Update:**
  ```bash
  cd ~/scalp && git pull && sudo systemctl restart scalp
  ```
