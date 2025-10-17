#!/usr/bin/env bash


hline() { printf '=%.0s' $(seq 1 ${1:-100}); }
# ================================================
# download um data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py aws_funding download-um-futures
# python datalake.py aws_funding verify-type-all um_futures
# python datalake.py aws_funding parse-type-all um_futures
# python datalake.py api_data download-recent-funding-type um_futures
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py aws_kline download-um-futures "1m"
# python datalake.py aws_kline verify-type-all um_futures "1m"
# python datalake.py aws_kline parse-type-all um_futures "1m"
# # python datalake.py api_data download-aws-missing-kline-type um_futures "1m"
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py generate kline-type binance um_futures "1m" --split-gaps --with-vwap --with-funding-rates
# python datalake.py generate resample-type binance um_futures "1h"
# ================================================
# download cm data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py aws_funding download-cm-futures
# python datalake.py aws_funding verify-type-all cm_futures
# python datalake.py aws_funding parse-type-all cm_futures
# python datalake.py api_data download-recent-funding-type cm_futures
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py aws_kline download-cm-futures "1m"
# python datalake.py aws_kline verify-type-all cm_futures "1m"
# python datalake.py aws_kline parse-type-all cm_futures "1m"
# python datalake.py api_data download-aws-missing-kline-type cm_futures "1m"
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py generate kline-type cm_futures "1m" --split-gaps --with-vwap --with-funding-rates
# python datalake.py generate resample-type cm_futures "1h" "0m"

# ================================================
# download spot data
# ================================================
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python datalake.py aws_kline download-spot "1m"
python datalake.py aws_kline verify-type-all spot "1m"
python datalake.py aws_kline parse-type-all spot "1m"
# python datalake.py api_data download-aws-missing-kline-type spot "1m"
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python datalake.py generate kline-type binance spot "1m" --split-gaps --with-vwap --no-with-funding-rates
python datalake.py generate resample-type binance spot "1h"

# ================================================
# download bybit data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py api_data download-bybit-klines-type
# python datalake.py api_data download-bybit-linear-funding-rates-type
# python datalake.py generate kline-type bybit um_futures "1m" --split-gaps --with-vwap --with-funding-rates
# python datalake.py generate resample-type bybit um_futures "1h"

# ================================================
# download okx data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python datalake.py generate kline-type okx um_futures "1m" --with-vwap --with-funding-rates
# python datalake.py generate resample-type okx um_futures "1h"