#!/usr/bin/env bash
# Pipeline for main.py (src/ aws_clone + aws_parse).
# Commands: clone | parse klines | parse funding | parse merge-wide

hline() { printf '=%.0s' $(seq 1 ${1:-100}); }

ROOT="$(dirname "$0")"
. "$ROOT/.venv/bin/activate"

# ================================================
# connect VPN
# ================================================
# "$ROOT/vpn_connect.sh"

# ================================================
# spot: clone + parse klines + merge-wide (no funding)
# ================================================
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python "$ROOT/main.py" clone "data/spot/daily/klines/*/1m/"
python "$ROOT/main.py" parse klines --trade-type spot --interval "1m"
python "$ROOT/main.py" parse merge-wide --trade-type spot --interval "1m"
# "${PY[@]}" parse merge-wide --trade-type spot --interval "${INTERVAL}"

# ================================================
# um_futures: clone klines + funding, then parse / merge
# ================================================
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python "$ROOT/main.py" clone "data/futures/um/daily/klines/*/1m/"
python "$ROOT/main.py" clone "data/futures/um/monthly/fundingRate/"
python "$ROOT/main.py" parse klines --trade-type um_futures --interval "1m"
python "$ROOT/main.py" parse funding --trade-type um_futures
python "$ROOT/main.py" parse merge-wide --trade-type um_futures --interval "1m"

# ================================================
# cm_futures (optional)
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# "${PY[@]}" clone "data/futures/cm/daily/klines/*/${INTERVAL}/"
# "${PY[@]}" clone "data/futures/cm/monthly/fundingRate/"
# "${PY[@]}" parse klines --trade-type cm_futures --interval "${INTERVAL}"
# "${PY[@]}" parse funding --trade-type cm_futures
# "${PY[@]}" parse merge-wide --trade-type cm_futures --interval "${INTERVAL}"

# ================================================
# optional flags (examples; uncomment to use)
# ================================================
# "${PY[@]}" clone "data/futures/um/daily/klines/BTCUSDT/${INTERVAL}/" --output-dir "$HOME/dev/babylake/binance_data/aws_data"
# "${PY[@]}" parse klines --trade-type um_futures --interval "${INTERVAL}" --force --symbols BTCUSDT,ETHUSDT
# "${PY[@]}" parse funding --trade-type um_futures --force --symbols BTCUSDT,ETHUSDT
# "${PY[@]}" parse merge-wide --trade-type um_futures --interval "${INTERVAL}" --force --symbols BTCUSDT,ETHUSDT

printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# sudo surfshark-vpn down
