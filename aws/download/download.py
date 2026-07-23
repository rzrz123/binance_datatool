from itertools import chain

from api.binance import BinanceMarketUMFapi
from aws.download.checksum import get_unverified_aws_data_files, verify_multi_process
from aws.download.client import AwsClient
from aws.download.symbol_filter import (
    filter_cm_futures_symbols,
    filter_spot_symbols,
    filter_um_futures_symbols,
)
from config import HTTP_TIMEOUT_SEC, N_JOBS, ContractType, DataFrequency, TradeType
from util.log_kit import logger
from util.network import create_aiohttp_session


def _verify_downloaded(
    trade_type: TradeType,
    product: str,
    symbols: list[str],
    time_interval: str | None = None,
):
    data_freq = DataFrequency.monthly if product == 'fundingRate' else DataFrequency.daily
    local_dir = AwsClient.LOCAL_DIR / AwsClient.get_base_dir(trade_type, product, data_freq)
    if time_interval is None:
        unverified_files = sorted(
            chain.from_iterable(get_unverified_aws_data_files(local_dir / symbol) for symbol in symbols)
        )
    else:
        unverified_files = sorted(
            chain.from_iterable(
                get_unverified_aws_data_files(local_dir / symbol / time_interval) for symbol in symbols
            )
        )

    if not unverified_files:
        logger.debug('All files verified')
        return

    logger.debug(f'num_unverified={len(unverified_files)}, n_jobs={N_JOBS}')
    num_success, num_fail = verify_multi_process(unverified_files)
    msg = f'{num_success} successfully verified'
    if num_fail > 0:
        msg += f', deleted {num_fail} corrupted files'
    logger.debug(msg)


async def download_aws_data(
    trade_type: TradeType,
    product: str,
    symbols: list[str],
    time_interval: str | None = None,
):
    if not symbols:
        return

    symbols = sorted(symbols)
    if time_interval is None:
        logger.debug(f'trade_type={trade_type.value}, num_symbols={len(symbols)}, {symbols[0]} -- {symbols[-1]}')
    else:
        logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}')

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        client = AwsClient(session=session, trade_type=trade_type, product=product, time_interval=time_interval)
        files = await client.batch_list_data_files(symbols)
        aws_files = list(chain.from_iterable(files.values()))
        client.aws_download(aws_files)

    _verify_downloaded(trade_type, product, symbols, time_interval=time_interval)


async def aws_list_symbols(trade_type: TradeType, product: str, time_interval: str | None = None) -> list[str]:
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        client = AwsClient(session=session, trade_type=trade_type, product=product, time_interval=time_interval)
        symbols = await client.list_symbols()
        if product == 'klines' and trade_type == TradeType.um_futures:
            api_client = BinanceMarketUMFapi(session)
            tradifi_symbols = await api_client.aioreq_list_tradifi_symbols()
            symbols = [symbol for symbol in symbols if symbol not in tradifi_symbols]
        return symbols


# === Funding wrappers ===


async def download_funding_rates(trade_type: TradeType, symbols: list[str]):
    if not symbols:
        return
    logger.debug('Start Download Funding Rates from Binance AWS')
    await download_aws_data(trade_type, 'fundingRate', symbols)


async def download_um_funding_rates(quote: str, contract_type: ContractType):
    logger.info('BHDS Download USDⓈ-M Futures Funding Rates')
    logger.debug(f'quote={quote}, contract_type={contract_type}')

    symbols = await aws_list_symbols(TradeType.um_futures, 'fundingRate')
    filtered_symbols = filter_um_futures_symbols(quote, contract_type, symbols)
    await download_funding_rates(trade_type=TradeType.um_futures, symbols=filtered_symbols)


async def download_cm_funding_rates(contract_type: ContractType):
    logger.info('BHDS Download Coin Futures Funding Rates')
    logger.debug(f'contract_type={contract_type}')

    symbols = await aws_list_symbols(TradeType.cm_futures, 'fundingRate')
    filtered_symbols = filter_cm_futures_symbols(contract_type, symbols)
    await download_funding_rates(trade_type=TradeType.cm_futures, symbols=filtered_symbols)


# === Kline wrappers ===


async def download_klines(trade_type: TradeType, time_interval: str, symbols: list[str]):
    await download_aws_data(trade_type, 'klines', symbols, time_interval=time_interval)


async def download_spot_klines(time_interval: str, quote: str, keep_stablecoins: bool, leverage_coins: bool):
    logger.info(
        f'BHDS Download Spot {time_interval} Klines, quote={quote}, '
        f'keep_stablecoins={keep_stablecoins}, keep_leverage_coins={leverage_coins}'
    )
    symbols = await aws_list_symbols(TradeType.spot, 'klines', time_interval)
    filtered_symbols = filter_spot_symbols(quote, keep_stablecoins, leverage_coins, symbols)
    await download_klines(trade_type=TradeType.spot, time_interval=time_interval, symbols=filtered_symbols)


async def download_um_klines(time_interval: str, quote: str, contract_type: ContractType):
    logger.info(f'BHDS Download USDⓈ-M Futures {time_interval} Klines, quote={quote}')
    symbols = await aws_list_symbols(TradeType.um_futures, 'klines', time_interval)
    filtered_symbols = filter_um_futures_symbols(quote, contract_type, symbols)
    await download_klines(trade_type=TradeType.um_futures, time_interval=time_interval, symbols=filtered_symbols)


async def download_cm_klines(time_interval: str, contract_type: ContractType):
    logger.info(f'BHDS Download COIN-M Futures {time_interval} Klines, contract_type={contract_type}')
    symbols = await aws_list_symbols(TradeType.cm_futures, 'klines', time_interval)
    filtered_symbols = filter_cm_futures_symbols(contract_type, symbols)
    await download_klines(trade_type=TradeType.cm_futures, time_interval=time_interval, symbols=filtered_symbols)
