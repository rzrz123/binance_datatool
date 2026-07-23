import asyncio
import subprocess
import tempfile
from pathlib import Path, PurePosixPath

import xmltodict
from aiohttp import ClientSession

from aws.download.util import split_into_batches
from config import BINANCE_DATA_DIR, DataFrequency, TradeType
from util.log_kit import logger
from util.network import async_retry_getter

_DEFAULT_DATA_FREQ = {
    'fundingRate': DataFrequency.monthly,
    'klines': DataFrequency.daily,
}


def run_aws_download(download_infos: list[tuple[str, Path]]):
    with tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='bhds_') as aria_file:
        for aws_url, local_file in download_infos:
            aria_file.write(f'{aws_url}\n  dir={local_file.parent}\n')
        aria_file.close()

        cmd = ['aria2c', '-i', aria_file.name, '-j32', '-x4', '-q']

        run_result = subprocess.run(cmd)
        returncode = run_result.returncode
    return returncode


def find_missings(download_infos: list[tuple[str, Path]]):
    return [(aws_url, local_file) for aws_url, local_file in download_infos if not local_file.exists()]


class AwsClient:
    PREFIX = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision'
    LOCAL_DIR = BINANCE_DATA_DIR / 'aws_data'
    TYPE_BASE_DIR = {
        TradeType.spot: PurePosixPath('data') / 'spot',
        TradeType.um_futures: PurePosixPath('data') / 'futures' / 'um',
        TradeType.cm_futures: PurePosixPath('data') / 'futures' / 'cm',
    }

    def __init__(
        self,
        session: ClientSession,
        trade_type: TradeType,
        product: str,
        time_interval: str | None = None,
        data_freq: DataFrequency | None = None,
    ):
        self.session = session
        self.trade_type = trade_type
        self.product = product
        self.time_interval = time_interval
        if data_freq is None:
            data_freq = _DEFAULT_DATA_FREQ[product]
        self.data_freq = data_freq
        self.base_dir = self.get_base_dir(trade_type, product, data_freq)

    async def _aio_get_xml(self, url):
        async with self.session.get(url) as resp:
            data = await resp.text()
        return xmltodict.parse(data)

    @classmethod
    def get_base_dir(cls, trade_type: TradeType, product: str, data_freq: DataFrequency) -> PurePosixPath:
        return cls.TYPE_BASE_DIR[trade_type] / data_freq.value / product

    def get_symbol_dir(self, symbol) -> PurePosixPath:
        symbol_dir = self.base_dir / symbol
        if self.time_interval is not None:
            symbol_dir = symbol_dir / self.time_interval
        return symbol_dir

    @classmethod
    def _get_aws_dir(cls, dir_path: PurePosixPath) -> str:
        return str(dir_path) + '/'

    async def list_dir(self, dir_path: PurePosixPath) -> list[PurePosixPath]:
        aws_dir_str = self._get_aws_dir(dir_path)
        base_url = url = f'{self.PREFIX}?delimiter=/&prefix={aws_dir_str}'
        results = []
        while True:
            data = await async_retry_getter(self._aio_get_xml, url=url)
            xml_data = data['ListBucketResult']
            if 'CommonPrefixes' in xml_data:
                results.extend([PurePosixPath(x['Prefix']) for x in xml_data['CommonPrefixes']])
            elif 'Contents' in xml_data:
                results.extend([PurePosixPath(x['Key']) for x in xml_data['Contents']])
            if xml_data['IsTruncated'] == 'false':
                break
            url = base_url + '&marker=' + xml_data['NextMarker']
        return sorted(results)

    async def list_symbols(self) -> list[str]:
        paths = await self.list_dir(self.base_dir)
        return sorted(p.name for p in paths)

    async def list_data_files(self, symbol: str) -> list[PurePosixPath]:
        return await self.list_dir(self.get_symbol_dir(symbol))

    async def batch_list_data_files(self, symbols: list[str]) -> dict[str, list[PurePosixPath]]:
        results = await asyncio.gather(*[self.list_data_files(symbol) for symbol in symbols])
        return dict(zip(symbols, results))

    def aws_download(self, aws_files: list[PurePosixPath], max_tries=3):
        logger.debug(f'Local file path: {self.LOCAL_DIR}')
        download_infos = [(f'{self.PREFIX}/{aws_file}', self.LOCAL_DIR / aws_file) for aws_file in aws_files]

        for try_id in range(max_tries):
            missing_infos = find_missings(download_infos)
            if not missing_infos:
                break

            logger.debug(f'try_id={try_id}, {len(missing_infos)} files to be downloaded', sep='-')
            batched_infos: list[tuple[str, Path]] = sorted(split_into_batches(missing_infos, 4096))
            for batch_idx, infos in enumerate(batched_infos, 1):
                logger.debug(
                    f'Download Batch{batch_idx}, '
                    f'num_files={len(infos)}, '
                    f'{infos[0][1].name} - {infos[-1][1].name}'
                )
                returncode = run_aws_download(infos)
                if returncode != 0:
                    logger.error(f'Batch{batch_idx}, Aria2 exited with code {returncode}')
