"""Step2: Arctic klines + funding -> ``klines_{interval}_wide``."""

from __future__ import annotations

from pathlib import Path

from tqdm import tqdm

from src.paths import (
    DEFAULT_ARCTIC_URI,
    DEFAULT_AWS_DATA_DIR,
    arctic_funding_symbol,
    arctic_klines_symbol,
    arctic_wide_symbol,
)
from util.log_kit import logger

from .parse_klines import collect_kline_zips_by_day
from .store import (
    day_bounds,
    get_library,
    has_data_in_range,
    read_range,
    update_multiindex,
)


def merge_wide(trade_type: str, interval: str, *, aws_data_dir: Path = DEFAULT_AWS_DATA_DIR, arctic_uri: str = DEFAULT_ARCTIC_URI, force: bool = False, symbols: list[str] | None = None) -> dict[str, int]:
    """Left-join funding onto klines by MultiIndex; write wide table day by day."""
    lib = get_library(trade_type, arctic_uri)
    klines_sym = arctic_klines_symbol(interval)
    funding_sym = arctic_funding_symbol()
    wide_sym = arctic_wide_symbol(interval)

    # Drive dates from local verified kline layout (same calendar as parse-klines).
    by_day = collect_kline_zips_by_day(aws_data_dir, trade_type, interval, symbols)
    days = sorted(by_day.keys())

    skipped = 0
    updated = 0
    rows = 0

    logger.info(f'merge-wide trade_type={trade_type} interval={interval} days={len(days)} force={force}')

    for day in tqdm(days, desc='merge-wide', ncols=100):
        # ================================================
        # 1. skip days already present (unless force)
        # ================================================
        start, end = day_bounds(day)
        if not force and has_data_in_range(lib, wide_sym, start, end):
            skipped += 1
            continue

        # ================================================
        # 2. left-join funding onto klines for the day
        # ================================================
        kline = read_range(lib, klines_sym, start, end)
        if kline.empty:
            logger.warning(f'merge-wide skip {day}: no klines in Arctic')
            continue

        if symbols is not None:
            kline = kline[kline.index.get_level_values('symbol').isin(symbols)]
            if kline.empty:
                continue

        funding = read_range(lib, funding_sym, start, end, columns=['funding_rate'])
        if funding.empty:
            wide = kline.copy()
            wide['funding_rate'] = 0.0
        else:
            if symbols is not None:
                funding = funding[funding.index.get_level_values('symbol').isin(symbols)]
            wide = kline.join(funding[['funding_rate']], how='left')
            wide['funding_rate'] = wide['funding_rate'].fillna(0.0)

        rows += update_multiindex(lib, wide_sym, wide, date_range=(start, end), replace_symbols=symbols)
        updated += 1

    logger.info(f'merge-wide done updated_days={updated} skipped_days={skipped} rows={rows}')
    return {'updated_days': updated, 'skipped_days': skipped, 'rows': rows}
