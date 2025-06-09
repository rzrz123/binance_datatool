import asyncio
import aiohttp
import json


from .log_kit import logger

BINANCE_CODES = {-1122, -1121}

class BinanceAPIException(Exception):

    def __init__(self, response, status_code, text):
        self.code = 0
        try:
            json_res = json.loads(text)
        except ValueError:
            self.message = 'Invalid JSON error message from Binance: {}'.format(response.text)
        else:
            self.code = json_res.get('code')
            self.message = json_res.get('msg')
        self.status_code = status_code
        self.response = response
        self.request = getattr(response, 'request', None)

    def __str__(self):  # pragma: no cover
        return 'APIError(code=%s): %s' % (self.code, self.message)


async def async_retry_getter(func, _max_times=5, _sleep_seconds=1, **kwargs):

    while True:
        try:
            return await func(**kwargs)
        except BinanceAPIException as e:
            if e.code in BINANCE_CODES:
                logger.warning(e)
                return None

            raise e
        except Exception as e:
            if _max_times == 0:
                logger.exception("Error occurred, 0 times retry left")
                raise e

            await asyncio.sleep(_sleep_seconds)
            _max_times -= 1
            _sleep_seconds *= 2


def create_aiohttp_session(timeout_sec):
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    session = aiohttp.ClientSession(timeout=timeout)
    return session
