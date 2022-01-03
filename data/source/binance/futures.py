import datetime
import json
import traceback
import urllib
import pytz
import collections
import os

import pandas as pd

from data.source.template import DataSource
from data.source.utils import download_as_df, FreqEnum
from utils.logging import log_print
from config import data_config
from dateutil.relativedelta import relativedelta


class BinanceFuturesServer(DataSource):
    _DEFAULT = {
        'start_datetime': datetime.datetime(2020, 1, 1, 0, 0, 0)
    }

    NAME = 'binance_futures'
    SUPPORTED = ['klines', 'trades']
    URL = {
        'info': 'https://fapi.binance.com/fapi/v1/exchangeInfo',
        'data_daily': 'https://data.binance.vision/data/futures/um/daily/{type}/{sym}/{file}',
        'data_monthly': 'https://data.binance.vision/data/futures/um/monthly/{type}/{sym}/{file}',
    }
    FREQ_MAP = {
        FreqEnum.M1: '1m',
        FreqEnum.M5: '5m',
        FreqEnum.M15: '15m',
        FreqEnum.M30: '30m',
        FreqEnum.H1: '60m',
        FreqEnum.H4: '4h',
        FreqEnum.D1: '1d',
    }

    SCHEMA = {
        'info': dict(
            order_by=['symbol'],
            columns=dict(
                symbol='String',
                contract_code='String',
                min_notional='Float64',
                price_tick='Float64',
                create_date='Date',
                status='UInt8',
            )
        ),
        'klines': dict(
            order_by=['datetime', 'symbol'],
            columns=dict(
                symbol='String',
                datetime='DateTime CODEC(DoubleDelta)',
                open='Float32 CODEC(Gorilla)',
                close='Float32 CODEC(Gorilla)',
                high='Float32 CODEC(Gorilla)',
                low='Float32 CODEC(Gorilla)',
                volume='Float64',
                turnover='Float64',
                count='UInt32 CODEC(Gorilla)',
                buy_volume='Float64',
                buy_turnover='Float64',
            ),
        ),
        'trades': dict(
            order_by=['timestamp'],
            columns=dict(
                id='UInt32 CODEC(Gorilla)',
                timestamp='UInt64 CODEC(DoubleDelta)',
                price='Float32 CODEC(Gorilla)',
                quantity='Float64',
                direction='Int8 CODEC(Gorilla)',
            ),
        )
    }

    def __init__(self):
        super(DataSource, self).__init__()

    def print(self, message):
        message = '[BinanceFutures] ' + message
        log_print(message)

    def convert_symbol(self, sym, to_standard=False):
        return sym

    def get_all_symbols(self):
        return self.get_info()['symbol'].tolist()

    def get_info(self):
        response = urllib.request.urlopen(self.URL['info']).read()
        response = json.loads(response)['symbols']
        for r in response:
            r['price_tick'] = float(r['filters'][0]['tickSize'])
            r['min_notional'] = float(r['filters'][-2]['notional'])
            r['status'] = True if r['status'].upper() == 'TRADING' else False
            r['onboardDate'] = pd.to_datetime(r['onboardDate'], unit='ms').date()
            _ = r.pop('filters')
        response = [x for x in response if not '_' in x['symbol']]
        res = pd.DataFrame(response)
        res = res[['symbol', 'pair', 'onboardDate', 'status', 'min_notional', 'price_tick']]
        res = res.rename(columns={
            'pair': 'contract_code',
            'onboardDate': 'create_date'}
        )
        return res

    def get_data(self, data_type, symbol, datetime, **kwargs):
        if data_type.lower() == 'klines':
            data = self._get_klines(symbol, datetime, **kwargs)
        elif data_type.lower() == 'trades':
            data = self._get_trades(symbol, datetime)
        return data

    def _get_klines(self, symbol, dt, freq=FreqEnum.M1, source_freq='daily', **kwargs):
        symbol = self.convert_symbol(symbol, to_standard=False)
        url = self.URL['data_{}'.format(source_freq)].format(
            type='klines',
            sym=symbol,
            file='{}/{}-{}-{}.zip'.format(
                self.FREQ_MAP[freq],
                symbol,
                self.FREQ_MAP[freq],
                dt.strftime('%Y-%m-%d') if source_freq == 'daily' else dt.strftime('%Y-%m')
            )
        )
        try:
            df, file_name = download_as_df(
                url,
                suffix='_{}_{}'.format(self.NAME, 'klines'),
                colnames=['datetime', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                          'turnover', 'count', 'buy_volume', 'buy_turnover', 'ignore'],
                usecols=['datetime', 'open', 'high', 'low', 'close', 'volume',
                         'turnover', 'count', 'buy_volume', 'buy_turnover'],

            )
            df['symbol'] = symbol
            df['count'] = df['count'].astype(int)
            df['datetime'] = pd.to_datetime(df['datetime'] + freq.value * 60 * 1000, unit='ms')
            os.remove(file_name)
        except:
            df = None
        return df

    def _get_trades(self, symbol, dt, source_freq='daily', **kwargs):
        symbol = self.convert_symbol(symbol, to_standard=False)
        url = self.URL['data_{}'.format(source_freq)].format(
            type='trades',
            sym=symbol,
            file='{}-trades-{}.zip'.format(
                symbol,
                dt.strftime('%Y-%m-%d') if source_freq == 'daily' else dt.strftime('%Y-%m')
            )
        )
        try:
            df, file_name = download_as_df(
                url,
                suffix='_{}_{}'.format(self.NAME, 'trades'),
                colnames=['id', 'price', 'quantity', 'turnover', 'timestamp', 'direction'],
                usecols=['id', 'price', 'quantity', 'timestamp', 'direction'],
                chunksize=kwargs.get('chunksize', None)
            )
            if not df is None:
                if not 'chunksize' in kwargs:
                    df['direction'] = ~df['direction'] * 2 - 1
                    os.remove(file_name)
                else:
                    def decorate(it, file_name):
                        for d in it:
                            d['direction'] = ~d['direction'] * 2 - 1
                            yield d
                        os.remove(file_name)

                    df = decorate(df, file_name)
        except:
            traceback.print_exc()
            df = None
        return df

    """ Sync clickhouse """

    def update(self, conn):
        conn.create_database(self.NAME)
        self._update_info(conn)
        self._update_klines(conn)
        self._update_trades(conn)


    def _update_info(self, conn):
        """ Function to update contract information """

        # build/check table
        table_name = '{}.{}_info'.format(self.NAME, self.NAME)
        self.print('Creating/Checking tables')
        conn.create_table(
            table_name=table_name,
            columns=self.SCHEMA['info'].get('columns'),
            order_by=self.SCHEMA['info'].get('order_by')
        )

        # get data
        df_to_update = self.get_info()
        df_to_update = df_to_update[self.SCHEMA['info']['columns'].keys()]

        # update database
        current_info = conn.query_df("SELECT * FROM {}".format(table_name))
        if len(current_info) > 0:
            df_to_update = df_to_update.loc[~df_to_update.symbol.isin(current_info.symbol)]
        if len(df_to_update) > 0:
            conn.insert_df(
                table_name=table_name,
                df=df_to_update
            )
        self.print('[{}] Inserted {} rows'.format(table_name, str(len(df_to_update))))

    def _update_klines(self, conn):
        """ Function to update market date """

        # build/check table
        table_name = '{}.{}_klines'.format(self.NAME, self.NAME)
        self.print('Creating/Checking tables')
        conn.create_table(
            table_name=table_name,
            columns=self.SCHEMA['klines'].get('columns'),
            order_by=self.SCHEMA['klines'].get('order_by')
        )

        symbols = self.get_all_symbols()
        cloud_dt = conn.exec('SELECT symbol, MAX(datetime) FROM binance_futures.binance_futures_klines GROUP BY symbol')
        cloud_dt = {x[0]: x[1] for x in cloud_dt}
        current_dt = datetime.date.today()
        for symbol in symbols:

            # find latest date in table
            query_dt = cloud_dt.get(symbol, self._DEFAULT['start_datetime']).date()

            # iterative update
            while query_dt <= current_dt:

                # if loading recent data, use daily batches
                if (query_dt.month == current_dt.month and query_dt.year == current_dt.year) \
                        or ((current_dt - query_dt).days < 30):
                    df_to_update = self._get_klines(
                        symbol=symbol,
                        dt=query_dt,
                        source_freq='daily'
                    )
                    query_dt += datetime.timedelta(days=1)

                # if loading historical data, use monthly batches
                else:
                    query_dt = query_dt.replace(day=1)
                    df_to_update = self._get_klines(
                        symbol=symbol,
                        dt=query_dt,
                        source_freq='monthly'
                    )
                    query_dt += relativedelta(months=1)

                # if data is available
                if isinstance(df_to_update, pd.DataFrame):
                    df_to_update['symbol'] = symbol
                    conn.insert_df(
                        table_name=table_name,
                        df=df_to_update
                    )
                    self.print('[{}] Inserted {} rows'.format(symbol, str(len(df_to_update))))


    def _update_trades(self, conn):
        """ Function to update market date """

        symbols = self.get_all_symbols()
        current_dt = datetime.date.today()
        for symbol in symbols:

            # build/check table
            table_name = '{}.{}_trades_{}'.format(self.NAME, self.NAME, symbol)
            self.print('Creating/Checking {}'.format(table_name))
            conn.create_table(
                table_name=table_name,
                columns=self.SCHEMA['trades'].get('columns'),
                order_by=self.SCHEMA['trades'].get('order_by')
            )

            # find latest date in table
            latest_dt = conn.exec("SELECT MAX(timestamp) FROM {}".format(table_name), drop_cache=True)[0][0]
            if latest_dt < 1:
                query_dt = self._DEFAULT.get('start_datetime').date()
            else:
                query_dt = pd.to_datetime(latest_dt, unit='ms').date() + datetime.timedelta(days=1)
            # log_print(query_dt)

            # iterative update
            while query_dt <= current_dt:

                # if loading recent data, use daily batches
                if (query_dt.month == current_dt.month and query_dt.year == current_dt.year) \
                        or ((current_dt - query_dt).days < 30):
                    df_to_update = self._get_trades(
                        symbol=symbol,
                        dt=query_dt,
                        chunksize=1000000,
                        source_freq='daily'
                    )
                    query_dt += datetime.timedelta(days=1)

                # if loading historical data, use monthly batches
                else:
                    query_dt = query_dt.replace(day=1)
                    df_to_update = self._get_trades(
                        symbol=symbol,
                        dt=query_dt,
                        chunksize=1000000,
                        source_freq='monthly'
                    )
                    query_dt += relativedelta(months=1)

                # if data is available
                if isinstance(df_to_update, collections.Iterable):
                    count = 0
                    for df_chunk in df_to_update:
                        inserted = conn.insert_df(
                            table_name=table_name,
                            df=df_chunk
                        )
                        count += inserted
                        self.print('[{}] Inserted {} rows'.format(symbol, str(count)))


class BinanceFuturesClient:

    def __init__(self, conn):
        self.tz = data_config['tzinfo']
        self.tzinfo = pytz.timezone(data_config['tzinfo'])
        self.conn = conn

    def get_info(self):
        current_info = self.conn.query_df("SELECT * FROM binance_futures.binance_futures_info")
        return current_info

    def get_klines(self, symbol, start_dt, end_dt, convert_tz=True):
        symbol = [symbol] if not isinstance(symbol, list) else symbol
        query = """
            SELECT *
            FROM  binance_futures.binance_futures_klines
                WHERE datetime BETWEEN '{start_dt}' AND '{end_dt}' AND symbol IN ({symbol})
                ORDER BY datetime 
        """.format(
            symbol=','.join("'{}'".format(x) for x in symbol),
            start_dt=self.tzinfo.localize(start_dt).astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S'),
            end_dt=self.tzinfo.localize(end_dt - datetime.timedelta(seconds=1)).astimezone(pytz.utc).strftime(
                '%Y-%m-%d %H:%M:%S'),
        )
        log_print(query, debug=True)
        data = self.conn.query_df(query)
        if convert_tz:
            data['datetime'] = data['datetime'].dt.tz_localize(pytz.utc)
            data['datetime'] = data['datetime'].dt.tz_convert(self.tzinfo)
        return data

    def get_trades(self, symbol, start_dt, end_dt, convert_tz=True):
        """ Function to query raw trades data """
        query = """
            SELECT *
            FROM  binance_futures.binance_futures_trades_{symbol}
                WHERE timestamp BETWEEN {start_dt} AND {end_dt}
                ORDER BY timestamp 
        """.format(
            symbol=symbol,
            start_dt=self.tzinfo.localize(start_dt).astimezone(pytz.utc).timestamp() * 1000 + 1,
            end_dt=self.tzinfo.localize(end_dt).astimezone(pytz.utc).timestamp() * 1000,
        )
        log_print(query, debug=True)
        data = self.conn.query_df(query)
        if convert_tz:
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize(pytz.utc)
            data['timestamp'] = data['timestamp'].dt.tz_convert(self.tzinfo)
        return data

    def get_agg_trades(self, symbol, start_dt, end_dt, convert_tz=True):
        """ Function to query aggregated trades data """
        query = """
            SELECT 
                timestamp,
                argMax(price, timestamp) AS price,
                sum(quantity) AS quantity,
                argMax(direction, timestamp) AS direction
            FROM  binance_futures.binance_futures_trades_{symbol}
                WHERE timestamp BETWEEN {start_dt} AND {end_dt}
                GROUP BY timestamp 
                ORDER BY timestamp 
        """.format(
            symbol=symbol,
            start_dt=self.tzinfo.localize(start_dt).astimezone(pytz.utc).timestamp() * 1000 + 1,
            end_dt=self.tzinfo.localize(end_dt).astimezone(pytz.utc).timestamp() * 1000,
        )
        log_print(query, debug=True)
        data = self.conn.query_df(query)
        if convert_tz:
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms').dt.tz_localize(pytz.utc)
            data['timestamp'] = data['timestamp'].dt.tz_convert(self.tzinfo)
        return data

    def get_agg_bars(self, symbol, start_dt, end_dt, interval=1, shift=0, fill_empty_interval=True, extra_fields=''):
        """ Function to query aggregated bars data """
        query = """
            SELECT toDateTime((agg_timestamp + 1) * {interval} + {shift}, '{tz}') AS datetime,
                   argMax(price, id) AS close,
                   argMin(price, id) AS open,
                   max(price) AS high,
                   min(price) AS low,
                   count(price) AS count,
                   sum(quantity) AS volume,
                   sum(price * quantity) AS total_turnover,
                   sum(if(direction=1, 1, 0)) AS buy_count,
                   sum(if(direction=1, quantity, 0)) AS buy_volume,
                   sum(if(direction=1, price * quantity, 0)) AS buy_turnover,
                   count - buy_count AS sell_count,
                   volume - buy_volume AS sell_volume,
                   total_turnover - buy_turnover AS sell_turnover
                   {extra_fields}
            FROM  binance_futures.binance_futures_trades_{symbol}
                WHERE timestamp BETWEEN {start_dt} + {shift} * 1000 AND {end_dt} + {shift} * 1000
                GROUP BY intDiv(timestamp + {shift} * 1000, {interval} * 1000) AS agg_timestamp
                ORDER BY datetime
        """.format(
            symbol=symbol,
            start_dt=self.tzinfo.localize(start_dt).astimezone(pytz.utc).timestamp() * 1000 + 1,  # to UTC time
            end_dt=self.tzinfo.localize(end_dt).astimezone(pytz.utc).timestamp() * 1000,  # to UTC time
            shift=str(shift),
            interval=str(interval),
            tz=self.tz,
            extra_fields=extra_fields
        )
        log_print(query, debug=True)
        data = self.conn.query_df(query)
        if fill_empty_interval:
            data = data.set_index('datetime').reindex(
                pd.date_range(
                    start=start_dt + datetime.timedelta(seconds=shift),
                    end=end_dt + datetime.timedelta(seconds=shift),
                    freq='{}s'.format(str(interval)),
                    tz=self.tzinfo
                )[1:]
            )
        return data


if __name__ == '__main__':


    # """ Clickhouse connection """
    # import datetime
    # from data import ClickhouseAPI
    # conn = ClickhouseAPI(local_mode=False)
    #
    # b = BinanceFuturesClient(conn)
    # df = b.get_trades(
    #     symbol='XRPUSDT',
    #     start_dt=datetime.datetime(2021, 10, 5),
    #     end_dt=datetime.datetime(2021, 10, 7),
    # )

    b = BinanceFuturesServer()
    df = b._get_klines('BTCUSDT', datetime.date(2021, 9, 20), source_freq='monthly')


