"""SHA256 verification for cloned Binance Vision zip + CHECKSUM pairs."""

import hashlib
import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from util.concurrent import mp_env_init
from util.log_kit import logger

from src.paths import verified_marker


def get_checksum_file(data_file: Path) -> Path:
    return data_file.parent / (data_file.name + '.CHECKSUM')


def verify_checksum(data_file: Path) -> tuple[bool, str | None]:
    checksum_path = get_checksum_file(data_file)
    if not checksum_path.exists():
        return False, 'Checksum file not exists'

    try:
        with open(checksum_path, 'r') as fin:
            text = fin.read()
        checksum_standard, _ = text.strip().split()
    except (OSError, ValueError):
        return False, 'Error reading checksum file'

    with open(data_file, 'rb') as file_to_check:
        checksum_value = hashlib.sha256(file_to_check.read()).hexdigest()

    if checksum_value != checksum_standard:
        return False, 'Checksum not equal'

    return True, None


def verify_aws_data_file(data_file: Path) -> bool:
    is_success, error = verify_checksum(data_file)

    if not is_success:
        logger.error(f'{error}, file={data_file}')
        checksum_file = get_checksum_file(data_file)
        data_file.unlink(missing_ok=True)
        checksum_file.unlink(missing_ok=True)
        return False

    verified_marker(data_file).touch()
    return True


def collect_unverified_zips(output_dir: Path, keys: list[str] | None = None) -> list[Path]:
    """Collect zip files that exist and lack a .verified marker.

    If keys is provided, only consider zip keys from that list (and their local paths).
    Otherwise scan output_dir recursively for *.zip.
    """
    unverified: list[Path] = []

    if keys is not None:
        for key in keys:
            if not key.endswith('.zip'):
                continue
            data_file = output_dir / key
            if data_file.exists() and not verified_marker(data_file).exists():
                unverified.append(data_file)
        return unverified

    for data_file in output_dir.rglob('*.zip'):
        if not verified_marker(data_file).exists():
            unverified.append(data_file)
    return unverified


def verify_multi_process(unverified_files: list[Path], n_jobs: int | None = None) -> tuple[int, int]:
    if not unverified_files:
        return 0, 0

    if n_jobs is None:
        n_jobs = max(1, (os.cpu_count() or 2) - 2)

    num_success, num_fail = 0, 0
    with ProcessPoolExecutor(max_workers=n_jobs, mp_context=mp.get_context('spawn'), initializer=mp_env_init) as exe:
        tasks = [exe.submit(verify_aws_data_file, path) for path in unverified_files]
        for task in as_completed(tasks):
            if task.result():
                num_success += 1
            else:
                num_fail += 1
    return num_success, num_fail
