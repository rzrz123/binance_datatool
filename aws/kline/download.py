from itertools import chain
from typing import List, Optional

from api.binance import BinanceMarketUMFapi
from aws.client_async import AwsKlineClient
from config import HTTP_TIMEOUT_SEC, ContractType, TradeType
from util.log_kit import logger
from util.network import create_aiohttp_session
from util.symbol_filter import (
    filter_cm_futures_symbols,
    filter_spot_symbols,
    filter_um_futures_symbols,
)


async def download_klines(trade_type: TradeType, time_interval: str, symbols: List[str], http_proxy: Optional[str]):
    if not symbols:
        return
    symbols = sorted(symbols)
    logger.debug(f'trade_type={trade_type.value}, time_interval={time_interval}, num_symbols={len(symbols)}')
    if http_proxy is not None:
        logger.debug(f'Use proxy, http_proxy={http_proxy}')

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        kline_client = AwsKlineClient(session=session, trade_type=trade_type, time_interval=time_interval, http_proxy=http_proxy) # type: ignore
        kline_files = await kline_client.batch_list_data_files(symbols)
        file_list = list(chain.from_iterable(kline_files.values()))
        kline_client.aws_download(file_list)


async def aws_list_kline_symbols(trade_type: TradeType, time_interval: str, http_proxy):
    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        kline_client = AwsKlineClient(session=session, trade_type=trade_type, time_interval=time_interval, http_proxy=http_proxy)
        symbols = await kline_client.list_symbols()
        if trade_type == TradeType.um_futures:
            api_client = BinanceMarketUMFapi(session, http_proxy)
            tradifi_symbols = await api_client.aioreq_list_tradifi_symbols()
            # remove symbols that are in tradifi_symbols
            symbols = [symbol for symbol in symbols if symbol not in tradifi_symbols]

    return symbols


async def download_spot_klines(time_interval: str, quote: str, keep_stablecoins: bool, leverage_coins: bool,http_proxy: Optional[str]):
    logger.info(f'BHDS Download Spot {time_interval} Klines, quote={quote}, keep_stablecoins={keep_stablecoins}, keep_leverage_coins={leverage_coins}')
    symbols = await aws_list_kline_symbols(TradeType.spot, time_interval, http_proxy)
    filtered_symbols = filter_spot_symbols(quote, keep_stablecoins, leverage_coins, symbols)
    await download_klines(trade_type=TradeType.spot,time_interval=time_interval,symbols=filtered_symbols,http_proxy=http_proxy)


async def download_um_klines(time_interval: str, quote: str, contract_type: ContractType,http_proxy: Optional[str]):
    logger.info(f'BHDS Download USDⓈ-M Futures {time_interval} Klines, quote={quote}')
    symbols = await aws_list_kline_symbols(TradeType.um_futures, time_interval, http_proxy)
    filtered_symbols = filter_um_futures_symbols(quote, contract_type, symbols)
    await download_klines(trade_type=TradeType.um_futures, time_interval=time_interval, symbols=filtered_symbols, http_proxy=http_proxy)


async def download_cm_klines(time_interval: str, contract_type: ContractType, http_proxy: Optional[str]):
    logger.info(f'BHDS Download COIN-M Futures {time_interval} Klines, contract_type={contract_type}')
    symbols = await aws_list_kline_symbols(TradeType.cm_futures, time_interval, http_proxy)
    filtered_symbols = filter_cm_futures_symbols(contract_type, symbols)
    await download_klines(trade_type=TradeType.cm_futures, time_interval=time_interval, symbols=filtered_symbols, http_proxy=http_proxy)
