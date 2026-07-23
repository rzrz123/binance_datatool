from config import DataFrequency, TradeType


def split_into_batches(arr: list, batch_size: int):
    return [arr[i : i + batch_size] for i in range(0, len(arr), batch_size)]


def local_list_funding_symbols(trade_type: TradeType):
    """List all locally available funding rate symbols."""
    from aws.download.client import AwsClient

    funding_dir = AwsClient.LOCAL_DIR / AwsClient.get_base_dir(
        trade_type=trade_type, product='fundingRate', data_freq=DataFrequency.monthly
    )
    return sorted(p.name for p in funding_dir.glob('*') if p.is_dir())


def local_list_kline_symbols(trade_type: TradeType, time_interval: str):
    from aws.download.client import AwsClient

    kline_dir = AwsClient.LOCAL_DIR / AwsClient.get_base_dir(
        trade_type=trade_type, product='klines', data_freq=DataFrequency.daily
    )
    return sorted(p.parts[-2] for p in kline_dir.glob(f'*/{time_interval}'))
