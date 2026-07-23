"""Async S3 listing and path-segment wildcard expansion for data.binance.vision."""

import asyncio

import xmltodict
from aiohttp import ClientSession

from src.paths import BASE_URL
from util.network import async_retry_getter

LIST_CONCURRENCY = 32


def _normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip().lstrip('/')
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    return prefix


def _as_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


class VisionLister:
    def __init__(self, session: ClientSession, concurrency: int = LIST_CONCURRENCY):
        self.session = session
        self._sem = asyncio.Semaphore(concurrency)

    async def _aio_get_xml(self, url: str) -> dict:
        async with self.session.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()
        return xmltodict.parse(text)

    async def list_dir(self, prefix: str) -> tuple[list[str], list[str]]:
        """List one level under prefix. Returns (common_prefixes, object_keys)."""
        prefix = _normalize_prefix(prefix)
        base_url = url = f'{BASE_URL}?delimiter=/&prefix={prefix}'
        common_prefixes: list[str] = []
        keys: list[str] = []

        while True:
            async with self._sem:
                data = await async_retry_getter(self._aio_get_xml, url=url)
            xml_data = data['ListBucketResult']

            for item in _as_list(xml_data.get('CommonPrefixes')):
                common_prefixes.append(item['Prefix'])

            for item in _as_list(xml_data.get('Contents')):
                key = item['Key']
                # Skip the directory placeholder key itself
                if key != prefix and not key.endswith('/'):
                    keys.append(key)

            if xml_data.get('IsTruncated') == 'false' or xml_data.get('IsTruncated') is False:
                break
            next_marker = xml_data.get('NextMarker')
            if not next_marker:
                break
            url = base_url + '&marker=' + next_marker

        return sorted(common_prefixes), sorted(keys)

    async def expand_stars(self, pattern: str) -> list[str]:
        """Expand path-segment `*` wildcards into concrete prefixes."""
        pattern = _normalize_prefix(pattern)
        segments = [s for s in pattern.split('/') if s != '']

        async def expand(current: str, remaining: list[str]) -> list[str]:
            if not remaining:
                return [_normalize_prefix(current)]

            head, *tail = remaining
            if head != '*':
                next_prefix = f'{current}{head}/' if current else f'{head}/'
                return await expand(next_prefix, tail)

            parent = current if current else ''
            common_prefixes, _ = await self.list_dir(parent)
            if not common_prefixes:
                return []

            tasks = [expand(cp, tail) for cp in common_prefixes]
            nested = await asyncio.gather(*tasks)
            return [p for group in nested for p in group]

        return await expand('', segments)

    async def list_keys_recursive(self, prefix: str) -> list[str]:
        """Recursively collect all object keys under prefix."""
        prefix = _normalize_prefix(prefix)
        common_prefixes, keys = await self.list_dir(prefix)
        if not common_prefixes:
            return keys

        tasks = [self.list_keys_recursive(cp) for cp in common_prefixes]
        nested = await asyncio.gather(*tasks)
        all_keys = list(keys)
        for group in nested:
            all_keys.extend(group)
        return sorted(all_keys)

    async def resolve_keys(self, pattern: str) -> list[str]:
        """Expand wildcards then recursively list all object keys. Public entry for listing."""
        prefixes = await self.expand_stars(pattern)
        if not prefixes:
            return []

        tasks = [self.list_keys_recursive(p) for p in prefixes]
        nested = await asyncio.gather(*tasks)
        return sorted({k for group in nested for k in group})
