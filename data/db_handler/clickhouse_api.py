import traceback
import logging
from clickhouse_driver import Client, connect
from sshtunnel import SSHTunnelForwarder
from config import data_config
from time import sleep
from functools import wraps
from utils.logging import log_print


def retry(func):
    """ Auto Retry """
    @wraps(func)
    def wrapper(*args, **kwargs):
        while True:
            try:
                res = func(*args, **kwargs)
                break
            except:
                traceback.print_exc()
                log_print('[Clickhouse] - Error Happened, retry in 10 seconds...')
                sleep(10)
        return res
    return wrapper


class ClickhouseAPI:

    def __init__(self, host='localhost', user='default', password='', local_mode=False):
        clickhouse_config = data_config['db_handler']['clickhouse']
        if not local_mode:
            self.server = SSHTunnelForwarder(
                (clickhouse_config['host'], clickhouse_config['port']),
                ssh_username=clickhouse_config['ssh']['username'],
                ssh_pkey=clickhouse_config['ssh']['key'],
                remote_bind_address=('localhost', 9000),
                local_bind_address=('localhost', 9000)
            )
            self.server.start()
        self.client = Client(
            host=host,
            user=user,
            password=password,
            settings={'date_time_input_format': 'best_effort'},
            connect_timeout=30,
            sync_request_timeout=15,
        )

    def __exit__(self):
        self.server.stop()

    def create_database(self, database_name):
        try:
            query = '''
                CREATE DATABASE IF NOT EXISTS {database_name}
            '''.format(
                database_name=database_name,
            )
            self.client.execute(query)
        except:
            logging.debug(query)
            logging.error(traceback.format_exc())
            # traceback.print_exc()
            raise RuntimeError('Create table failed')

    def create_table(self, table_name, columns, order_by=None, s3_policy=False):
        try:
            query = '''
                CREATE TABLE IF NOT EXISTS {table_name}
                (
                    {columns}
                ) ENGINE = MergeTree
                {order_by}
                {s3_policy}
            '''.format(
                table_name=table_name,
                columns=','.join(' '.join([k, v]) for k, v in columns.items()),
                order_by='ORDER BY ({})'.format(','.join(order_by)) if order_by is not None else '',
                s3_policy="SETTINGS storage_policy = 's3'" if s3_policy else "",
            )
            self.client.execute(query)
        except:
            logging.debug(query)
            logging.error(traceback.format_exc())
            # traceback.print_exc()
            raise RuntimeError('Create table failed')

    @retry
    def exec(self, query, drop_cache=False):
        res = self.client.execute(query)
        if drop_cache:
            self.client.execute('SYSTEM DROP MARK CACHE')
        return res

    @retry
    def insert_df(self, table_name, df):
        res = self.client.execute('INSERT INTO {} VALUES'.format(table_name), df.to_dict('records'))
        return res

    @retry
    def query_df(self, query):
        res = self.client.query_dataframe(query)
        return res

