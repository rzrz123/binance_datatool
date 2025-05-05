#!/usr/bin/env bash

# interval=${1:-1m}

# ================================================
# download um data
# ================================================
python bhds.py aws_funding download-um-futures
python bhds.py aws_funding verify-type-all um_futures
python bhds.py aws_funding parse-type-all um_futures

python bhds.py aws_kline download-um-futures "1m"
python bhds.py aws_kline verify-type-all um_futures "1m"
python bhds.py aws_kline parse-type-all um_futures "1m"
python bhds.py api_data download-aws-missing-kline-type um_futures "1m"

python bhds.py generate kline-type um_futures "1m" --split-gaps --with-vwap --with-funding-rates
python bhds.py generate resample-type um_futures "1h" "0m"
# ================================================
# download cm data
# ================================================
# python bhds.py aws_funding download-cm-futures
