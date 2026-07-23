#!/usr/bin/env bash

hline() { printf '=%.0s' $(seq 1 ${1:-100}); }

ROOT="$(dirname "$0")"
. "$ROOT/.venv/bin/activate"

# ================================================
# connect VPN
# ================================================
"$ROOT/vpn_connect.sh"
# ================================================
# download spot data
# ================================================
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python "$ROOT/datalake.py" aws_download spot-klines "1m"
python "$ROOT/datalake.py" aws_parse klines spot "1m"
python "$ROOT/datalake.py" generate kline-type spot "1m" --split-gaps --with-vwap --no-with-funding-rates
# python "$ROOT/datalake.py" generate resample-type spot "1h" "0m"
# ================================================
# download um data
# ================================================
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python "$ROOT/datalake.py" aws_download um-funding
python "$ROOT/datalake.py" aws_parse funding um_futures
python "$ROOT/datalake.py" api_data download-recent-funding-type um_futures
python "$ROOT/datalake.py" aws_download um-klines "1m"
python "$ROOT/datalake.py" aws_parse klines um_futures "1m"
python "$ROOT/datalake.py" generate kline-type um_futures "1m" --split-gaps --with-vwap --with-funding-rates
python "$ROOT/datalake.py" generate resample-type um_futures "1h" "0m"
# ================================================
# download cm data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python "$ROOT/datalake.py" aws_download cm-funding
# python "$ROOT/datalake.py" aws_parse funding cm_futures
# python "$ROOT/datalake.py" api_data download-recent-funding-type cm_futures
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python "$ROOT/datalake.py" aws_download cm-klines "1m"
# python "$ROOT/datalake.py" aws_parse klines cm_futures "1m"
# python "$ROOT/datalake.py" api_data download-aws-missing-kline-type cm_futures "1m"
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python "$ROOT/datalake.py" generate kline-type cm_futures "1m" --split-gaps --with-vwap --with-funding-rates
# python "$ROOT/datalake.py" generate resample-type cm_futures "1h" "0m"

# ================================================
# disconnect VPN
# ================================================
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
sudo surfshark-vpn down
