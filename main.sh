#!/usr/bin/env bash


hline() { printf '=%.0s' $(seq 1 ${1:-100}); }
# ================================================
# download um data
# ================================================
# 用函数动态生成指定长度的分隔线（默认30个=）
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python bhds.py aws_funding download-um-futures
python bhds.py aws_funding verify-type-all um_futures
python bhds.py aws_funding parse-type-all um_futures
python bhds.py api_data download-recent-funding-type um_futures
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python bhds.py aws_kline download-um-futures "1m"
python bhds.py aws_kline verify-type-all um_futures "1m"
python bhds.py aws_kline parse-type-all um_futures "1m"
# python bhds.py api_data download-aws-missing-kline-type um_futures "1m"
printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
python bhds.py generate kline-type um_futures "1m" --split-gaps --with-vwap --with-funding-rates
python bhds.py generate resample-type um_futures "1h" "0m"
# ================================================
# download cm data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python bhds.py aws_funding download-cm-futures
# python bhds.py aws_funding verify-type-all cm_futures
# python bhds.py aws_funding parse-type-all cm_futures
# python bhds.py api_data download-recent-funding-type cm_futures
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python bhds.py aws_kline download-cm-futures "1m"
# python bhds.py aws_kline verify-type-all cm_futures "1m"
# python bhds.py aws_kline parse-type-all cm_futures "1m"
# python bhds.py api_data download-aws-missing-kline-type cm_futures "1m"
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python bhds.py generate kline-type cm_futures "1m" --split-gaps --with-vwap --with-funding-rates
# python bhds.py generate resample-type cm_futures "1h" "0m"

# ================================================
# download spot data
# ================================================
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python bhds.py aws_kline download-spot "1m"
# python bhds.py aws_kline verify-type-all spot "1m"
# python bhds.py aws_kline parse-type-all spot "1m"
# python bhds.py api_data download-aws-missing-kline-type spot "1m"
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"
# python bhds.py generate kline-type spot "1m" --split-gaps --with-vwap --no-with-funding-rates
# python bhds.py generate resample-type spot "1h" "0m"
# printf '\e[32m%s\e[0m | %s |\n' "$(date +%T)" "$(hline)"