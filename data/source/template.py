import datetime
import logging
from abc import ABC


class DataSource(ABC):

    _DEFAULT = {
        'start_datetime': datetime.datetime(2020, 10, 1, 0, 0, 0)
    }
    def convert_symbol(self, sym):
        return sym

    def create_tables(self, conn, table_name, table_schema):
        """ Function to create table if not exist """
        logging.info('Creating/Checking tables')
        conn.create_table(
            table_name=table_name,
            columns=table_schema.get('columns'),
            order_by=table_schema.get('order_by')
        )

