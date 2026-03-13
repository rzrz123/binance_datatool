import asyncio
import pandas as pd

from util.log_kit import logger
from api.binance import BinanceBaseApi
from util.network import create_aiohttp_session


class OkxSwapApi(BinanceBaseApi):
    PREFIX = 'https://www.okx.com'

    async def aioreq_klines(self, **kwargs) -> list:
        url = f'{self.PREFIX}/api/v5/market/history-candles'
        return await self._aio_get(url, **kwargs)

    async def aioreq_funding_rate(self, **kwargs):
        pass

    async def aioreq_instruments_info(self, **kwargs):
        url = f'{self.PREFIX}/api/v5/public/instruments'
        return await self._aio_get(url, kwargs)

async def get_okx_swap_instruments_info() -> list[dict]:
    logger.info("Getting Okx swap instruments info")
    async with create_aiohttp_session(15) as session:
        res = await OkxSwapApi(session, '').aioreq_instruments_info(instType='SWAP')
        return res['data']

if __name__ == '__main__':
    res = asyncio.run(get_okx_swap_instruments_info())
    print(res)