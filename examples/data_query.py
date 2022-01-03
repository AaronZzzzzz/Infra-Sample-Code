
if __name__ == '__main__':

    """ Clickhouse connection """
    import datetime
    from data import ClickhouseAPI
    conn = ClickhouseAPI()

    """ HuobiFutures """
    from data.source import HuobiFuturesClient
    hfc = HuobiFuturesClient(conn)
    start_dt = datetime.datetime(2021, 9, 8)
    end_dt = datetime.datetime(2021, 9, 9)

    # get klines data
    res = hfc.get_klines(['BTCUSDT'], start_dt, end_dt, convert_tz=True)

    # get trades data
    res = hfc.get_trades('BTCUSDT', start_dt, end_dt, convert_tz=True)

    # get aggregated trades data
    res = hfc.get_agg_trades('BTCUSDT', start_dt, end_dt, interval=60, shift=15, fill_empty_interval=True)

    # get aggregated trades with extra fields
    res = hfc.get_agg_trades(
        symbol='BTCUSDT',
        start_dt=start_dt,
        end_dt=end_dt,
        interval=60,
        shift=15,
        fill_empty_interval=True,
        extra_fields="""
            , sum(amount * price) / sum(amount) AS vwap
        """
    )

    """ BinanceFutures """
    from data.source import BinanceFuturesClient
    bfc = BinanceFuturesClient(conn)
    start_dt = datetime.datetime(2021, 6, 1)
    end_dt = datetime.datetime(2021, 6, 3)

    # get klines data
    res = bfc.get_klines(['BTCUSDT'], start_dt, end_dt, convert_tz=True)

    # get trades data
    res = bfc.get_trades('BTCUSDT', start_dt, end_dt, convert_tz=True)

    # get aggregated trades data
    res = bfc.get_agg_trades('BTCUSDT', start_dt, end_dt, interval=60, shift=15, fill_empty_interval=True)

    # get aggregated trades with extra fields
    res = bfc.get_agg_trades(
        symbol='BTCUSDT',
        start_dt=start_dt,
        end_dt=end_dt,
        interval=60,
        shift=15,
        fill_empty_interval=True,
        extra_fields="""
            , sum(amount * price) / sum(amount) AS vwap
        """
    )

