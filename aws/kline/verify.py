from itertools import chain
from typing import List

from aws.kline.util import local_list_kline_symbols
import config
from aws.checksum import get_unverified_aws_data_files, verify_multi_process
from aws.client_async import AwsKlineClient
from config import DataFrequency, TradeType
from util.log_kit import logger


def verify_klines(trade_type: TradeType, time_interval: str, symbols: List[str]):
    logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}')

    local_kline_dir = AwsKlineClient.LOCAL_DIR / AwsKlineClient.get_base_dir(trade_type, DataFrequency.daily)
    unverified_files = sorted(
        chain.from_iterable(
            get_unverified_aws_data_files(local_kline_dir / symbol / time_interval) for symbol in symbols))

    if not unverified_files:
        logger.debug('All files verified')
        return

    logger.debug(f'num_unverified={len(unverified_files)}, n_jobs={config.N_JOBS}')

    num_success, num_fail = verify_multi_process(unverified_files)

    msg = f'{num_success} successfully verified'
    if num_fail > 0:
        msg += f', deleted {num_fail} corrupted files'
    logger.debug(msg)


def verify_all_klines(trade_type: TradeType, time_interval: str):
    logger.info(f'BHDS verify {trade_type.value} {time_interval} Klines')
    symbols = local_list_kline_symbols(trade_type, time_interval)
    verify_klines(trade_type, time_interval, symbols)
