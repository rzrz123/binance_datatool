"""Clone Binance Vision prefixes: list → incremental aria2c download → SHA256 verify."""

import os
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from src.paths import BASE_URL, DEFAULT_AWS_DATA_DIR, verified_marker
from util.log_kit import logger
from util.network import create_aiohttp_session

from .checksum import collect_unverified_zips, verify_multi_process
from .listing import VisionLister

MAX_DOWNLOAD_TRIES = 3
N_VERIFY_JOBS = max(1, (os.cpu_count() or 2) - 2)
HTTP_TIMEOUT_SEC = 15
ARIA2_BATCH_SIZE = 4096
ARIA2_MAX_CONCURRENT = 32
ARIA2_CONNECTIONS_PER_SERVER = 4


@dataclass(frozen=True)
class CloneResult:
    prefix: str
    listed: int
    already_present: int
    to_download: int
    missing_after_download: int
    verified_ok: int
    verified_fail: int


@dataclass(frozen=True)
class _DownloadResult:
    total: int
    already_present: int
    queued: int
    still_missing: int


def _split_into_batches(arr: list, batch_size: int) -> list[list]:
    return [arr[i : i + batch_size] for i in range(0, len(arr), batch_size)]


def _is_complete_local(local_file: Path) -> bool:
    """True if this key does not need to be downloaded again."""
    name = local_file.name

    if name.endswith('.CHECKSUM'):
        zip_path = local_file.with_name(name[: -len('.CHECKSUM')])
        if zip_path.exists() and zip_path.stat().st_size > 0 and verified_marker(zip_path).exists():
            return True
        return local_file.exists() and local_file.stat().st_size > 0

    if name.endswith('.zip'):
        if not local_file.exists() or local_file.stat().st_size == 0:
            return False
        return True

    return local_file.exists() and local_file.stat().st_size > 0


def _find_missings(download_infos: list[tuple[str, Path]]) -> list[tuple[str, Path]]:
    return [(url, path) for url, path in download_infos if not _is_complete_local(path)]


def _run_aria2_download(download_infos: list[tuple[str, Path]]) -> int:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, prefix='aws_clone_') as aria_file:
        for aws_url, local_file in download_infos:
            local_file.parent.mkdir(parents=True, exist_ok=True)
            aria_file.write(f'{aws_url}\n  dir={local_file.parent}\n  out={local_file.name}\n')
        aria_file.flush()
        aria_path = aria_file.name

    cmd = ['aria2c', '-i', aria_path, f'-j{ARIA2_MAX_CONCURRENT}', f'-x{ARIA2_CONNECTIONS_PER_SERVER}', '-q']
    return subprocess.run(cmd).returncode


def _build_download_infos(keys: list[str], output_dir: Path) -> list[tuple[str, Path]]:
    return [(f'{BASE_URL}/{key}', output_dir / key) for key in keys]


def _aws_download(keys: list[str], output_dir: Path, max_tries: int = MAX_DOWNLOAD_TRIES) -> _DownloadResult:
    """Download only missing/incomplete keys. Safe to re-run for incremental sync."""
    download_infos = _build_download_infos(keys, output_dir)
    total, missing_infos = len(download_infos), _find_missings(download_infos)
    already_present, queued = total - len(missing_infos), len(missing_infos)

    logger.info(f'Local output: {output_dir} | remote={total} already_present={already_present} to_download={queued}')

    if not missing_infos:
        logger.info('Nothing to download (local already up to date for listed keys)')
        return _DownloadResult(total=total, already_present=already_present, queued=0, still_missing=0)

    for try_id in range(max_tries):
        missing_infos = _find_missings(download_infos)
        if not missing_infos:
            break

        logger.info(f'try_id={try_id}, {len(missing_infos)} files to download')
        batches = _split_into_batches(sorted(missing_infos, key=lambda x: str(x[1])), ARIA2_BATCH_SIZE)
        for batch_idx, infos in enumerate(batches, 1):
            logger.info(f'Download batch {batch_idx}/{len(batches)}, num_files={len(infos)}, {infos[0][1].name} - {infos[-1][1].name}')
            returncode = _run_aria2_download(infos)
            if returncode != 0:
                logger.error(f'Batch {batch_idx}, aria2c exited with code {returncode}')

    still_missing = _find_missings(download_infos)
    if still_missing:
        logger.error(f'{len(still_missing)} files still missing after {max_tries} tries')
    else:
        logger.info('All listed keys present locally')

    return _DownloadResult(total=total, already_present=already_present, queued=queued, still_missing=len(still_missing))


async def clone(prefix: str, output_dir: Path = DEFAULT_AWS_DATA_DIR) -> CloneResult:
    """Clone all objects under a Binance Vision S3 prefix (supports path-segment `*`).

    Re-runs are incremental: remote keys are listed, compared to local files,
    and only missing/incomplete objects are downloaded. Already-verified zips
    are not re-checked.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f'Clone prefix={prefix!r} → {output_dir}')

    async with create_aiohttp_session(HTTP_TIMEOUT_SEC) as session:
        lister = VisionLister(session=session)
        keys = await lister.resolve_keys(prefix)

    logger.info(f'Listed {len(keys)} objects')
    if not keys:
        return CloneResult(prefix=prefix, listed=0, already_present=0, to_download=0, missing_after_download=0, verified_ok=0, verified_fail=0)

    # download
    dl = _aws_download(keys, output_dir, max_tries=MAX_DOWNLOAD_TRIES)

    # verify
    unverified = collect_unverified_zips(output_dir, keys=keys)
    logger.info(f'Verifying {len(unverified)} zip files (skip already .verified)')
    verified_ok, verified_fail = verify_multi_process(unverified, n_jobs=N_VERIFY_JOBS)
    logger.info(f'Verify done: ok={verified_ok}, fail={verified_fail}')

    return CloneResult(
        prefix=prefix,
        listed=len(keys),
        already_present=dl.already_present,
        to_download=dl.queued,
        missing_after_download=dl.still_missing,
        verified_ok=verified_ok,
        verified_fail=verified_fail,
    )
