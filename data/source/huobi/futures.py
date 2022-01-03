
import datetime
import json
import urllib
import pytz

import pandas as pd

from data.source.template import DataSource
from data.source.utils import download_as_df, FreqEnum
from utils.logging import log_print
from config import data_config

class HuobiFuturesServer(DataSource):

    NAME = 'huobi_futures'
    SUPPORTED = ['index-klines', 'klines', 'mark-price-klines', 'trades']
    URL = {
        'info': 'https://api.hbdm.com/linear-swap-api/v1/swap_contract_info',
        'data': 'https://futures.huobi.com/data/{type}/linear-swap/daily/{sym}/{file}',
    }
    FREQ_MAP = {
        FreqEnum.M1: '1min',
        FreqEnum.M5: '5min',
        FreqEnum.M15: '15min',
        FreqEnum.M30: '30min',
        FreqEnum.H1: '60min',
        FreqEnum.H4: '4hour',
        FreqEnum.D1: '1day',
    }


    SCHEMA = {
        'info': dict(
            order_by=['symbol'],
            columns=dict(
                symbol='String',
                contract_code='String',
                contract_size='Float64',
                price_tick='Float64',
                create_date='Date',
                contract_status='Int64',
                support_margin_mode='String',
            )
        ),
        'klines': dict(
            order_by=['datetime', 'symbol'],
            columns=dict(
                symbol='String',
                datetime='Datetime',
                open='Float64',
                close='Float64',
                high='Float64',
                low='Float64',
                volume='Float64',
                amount='Float64',
            ),
        ),
        'trades': dict(
            order_by=['timestamp'],
            columns=dict(
                id='UInt64',
                timestamp='UInt64',
                price='Float64',
                amount='Float64',
                quantity='Float64',
                turnover='Float64',
                direction='Int8',
            ),
        )
    }

    def __init__(self):
        super(DataSource, self).__init__()

    def convert_symbol(self, sym, to_standard=False):
        if to_standard:
            return ''.join(sym.split('-')).upper()
        else:
            return sym[:-4].upper() + '-' + 'USDT'

    def get_all_symbols(self):
        response = urllib.request.urlopen(self.URL['info']).read()
        return list(map(lambda symbol: self.convert_symbol(symbol['contract_code'], to_standard=True), json.loads(response)['data']))

    def get_info(self):
        response = urllib.request.urlopen(self.URL['info']).read()
        return pd.DataFrame(json.loads(response)['data'])

    def get_data(self, data_type, symbol, datetime, **kwargs):
        if data_type.lower() == 'klines':
            data = self._get_klines(symbol, datetime, **kwargs)
        elif data_type.lower() == 'trades':
            data = self._get_trades(symbol, datetime)
        return data

    def _get_klines(self, symbol, dt, freq=FreqEnum.M1, **kwargs):
        symbol = self.convert_symbol(symbol, to_standard=False)
        url = self.URL['data'].format(
            type='klines',
            sym=symbol,
            file='{}/{}-{}-{}.zip'.format(
                self.FREQ_MAP[freq],
                symbol,
                self.FREQ_MAP[freq],
                dt.strftime('%Y-%m-%d')
            )
        )
        try:
            df = download_as_df(url, suffix='_{}_{}'.format(self.NAME, 'klines'))
            df = df.rename(columns={
                0: 'datetime',
                1: 'open',
                2: 'close',
                3: 'high',
                4: 'low',
                5: 'volume',
                6: 'amount'
            })
            df['datetime'] = pd.to_datetime(df['datetime'] + freq.value * 60, unit='s')
        except:
            df = None
        return df

    def _get_trades(self, symbol, dt, **kwargs):
        symbol = self.convert_symbol(symbol, to_standard=False)
        url = self.URL['data'].format(
            type='trades',
            sym=symbol,
            file='{}-trades-{}.zip'.format(symbol, dt.strftime('%Y-%m-%d'))
        )
        try:
            df = download_as_df(url, suffix='_{}_{}'.format(self.NAME, 'trades'))
            df = df.rename(columns={
                0: 'id',
                1: 'timestamp',
                2: 'price',
                3: 'amount',
                4: 'quantity',
                5: 'turnover',
                6: 'direction'
            })
            df['direction'] = (df['direction'] == 'buy') * 2 - 1
        except:
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
        table_name = '{}.info'.format(self.NAME)
        log_print('Creating/Checking tables')
        conn.create_table(
            table_name=table_name,
            columns=self.SCHEMA['info'].get('columns'),
            order_by=self.SCHEMA['info'].get('order_by')
        )

        # get data
        df_to_update = self.get_info()
        df_to_update['create_date'] = pd.to_datetime(df_to_update['create_date'])
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
        log_print('[{}] Inserted {} rows'.format(table_name, str(len(df_to_update))))

    def _update_klines(self, conn):
        """ Function to update market date """

        # build/check table
        table_name = '{}.klines'.format(self.NAME)
        log_print('Creating/Checking tables')
        conn.create_table(
            table_name=table_name,
            columns=self.SCHEMA['klines'].get('columns'),
            order_by=self.SCHEMA['klines'].get('order_by')
        )

        symbols = self.get_all_symbols()
        current_dt = datetime.date.today()
        for symbol in symbols:
            latest_dt = conn.exec("SELECT MAX(datetime) FROM {} WHERE symbol='{}'".format(table_name, symbol))[0][0]
            query_dt = self._DEFAULT.get('start_datetime').date() if latest_dt < self._DEFAULT['start_datetime'] else latest_dt.date() + datetime.timedelta(days=1)
            log_print(query_dt)
            while query_dt <= current_dt:
                df_to_update = self._get_klines(
                    symbol=symbol,
                    dt=query_dt,
                )
                if isinstance(df_to_update, pd.DataFrame):
                    df_to_update['symbol'] = symbol
                    conn.insert_df(
                        table_name=table_name,
                        df=df_to_update
                    )
                    log_print('[{}] Inserted {} rows'.format(symbol, str(len(df_to_update))))
                query_dt += datetime.timedelta(days=1)

    def _update_trades(self, conn):
        """ Function to update market date """

        symbols = self.get_all_symbols()
        current_dt = datetime.date.today()
        for symbol in symbols:

            # build/check table
            table_name = '{}.trades_{}'.format(self.NAME, symbol)
            log_print('Creating/Checking {}'.format(table_name))
            conn.create_table(
                table_name=table_name,
                columns=self.SCHEMA['trades'].get('columns'),
                order_by=self.SCHEMA['trades'].get('order_by')
            )

            latest_dt = conn.exec("SELECT MAX(timestamp) FROM {}".format(table_name))[0][0]
            if latest_dt < 1:
                query_dt = self._DEFAULT.get('start_datetime').date()
            else:
                query_dt = pd.to_datetime(latest_dt, unit='ms').date() + datetime.timedelta(days=1)
            log_print(query_dt)
            while query_dt <= current_dt:
                df_to_update = self._get_trades(
                    symbol=symbol,
                    dt=query_dt,
                )
                if isinstance(df_to_update, pd.DataFrame):
                    inserted = conn.insert_df(
                        table_name=table_name,
                        df=df_to_update
                    )
                    log_print('[{}] Inserted {} rows'.format(symbol, str(inserted)))
                query_dt += datetime.timedelta(days=1)


class HuobiFuturesClient:

    def __init__(self, conn):
        self.tz = data_config['tzinfo']
        self.tzinfo = pytz.timezone(data_config['tzinfo'])
        self.conn = conn

    def get_info(self):
        current_info = self.conn.query_df("SELECT * FROM huobi_futures.info")
        return current_info

    def get_klines(self, symbol, start_dt, end_dt, convert_tz=True):
        symbol = [symbol] if not isinstance(symbol, list) else symbol
        query = """
            SELECT *
            FROM  huobi_futures.klines
                WHERE datetime BETWEEN '{start_dt}' AND '{end_dt}' AND symbol IN ({symbol})
                ORDER BY datetime 
        """.format(
            symbol=','.join("'{}'".format(x) for x in symbol),
            start_dt=self.tzinfo.localize(start_dt).astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S'),
            end_dt=self.tzinfo.localize(end_dt - datetime.timedelta(seconds=1)).astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S'),
        )
        log_print(query, debug=True)
        data = self.conn.query_df(query)
        data['datetime'] = data['datetime'] + datetime.timedelta(minutes=1)
        if convert_tz:
            data['datetime'] = data['datetime'].dt.tz_localize(pytz.utc)
            data['datetime'] = data['datetime'].dt.tz_convert(self.tzinfo)
        return data

    def get_trades(self, symbol, start_dt, end_dt, convert_tz=True):
        """ Function to query raw trades data """
        query = """
            SELECT *
            FROM  huobi_futures.trades_{symbol}
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

    def get_agg_trades(self, symbol, start_dt, end_dt, interval=1, shift=0, fill_empty_interval=True, extra_fields=''):
        """ Function to query aggregated trades data """
        query = """
            SELECT toDateTime((agg_timestamp + 1) * {interval} + {shift}, '{tz}') AS datetime,
                   argMax(price, timestamp) AS close,
                   argMin(price, timestamp) AS open,
                   max(price) AS high,
                   min(price) AS low,
                   count(price) AS total_count,
                   sum(amount) AS total_amount,
                   sum(quantity) AS total_quantity,
                   sum(turnover) AS total_turnover,
                   sum(if(direction=1, 1, 0)) AS buy_count,
                   sum(if(direction=1, amount, 0)) AS buy_amount,
                   sum(if(direction=1, quantity, 0)) AS buy_quantity,
                   sum(if(direction=1, turnover, 0)) AS buy_turnover,
                   total_count - buy_count AS sell_count,
                   total_amount - buy_amount AS sell_amount,
                   total_quantity - buy_quantity AS sell_quantity,
                   total_turnover - buy_turnover AS sell_turnover
                   {extra_fields}
            FROM  huobi_futures.trades_{symbol}
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
