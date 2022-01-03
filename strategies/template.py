import datetime
import sys
import json
import numpy as np
from collections import deque
from loguru import logger
from exchange.enums import OrderStatus


def base36(num):
    return ((num == 0) and "0") or (base36(num // 36).lstrip("0") + "0123456789abcdefghijklmnopqrstuvwxyz"[num % 36])


class BaseStrategy:

    def __init__(self, params=None, log_level=0):
        params = params if params is not None else {}

        # switch of logging
        self.verbose = params.get('verbose', True)

        # exchange protocol
        self.exchange = None

        self.execution = None

        # save account information (capital, shares, etc.,)
        self.account = dict()

        # log level
        self.log_level = int(log_level)

        # logger to record all intermediate data
        self.id = base36(int(datetime.datetime.now().strftime('%y%m%d%H%M%S')))
        self.config_logger(logger)

    """ Helper Functions """

    def config_logger(self, logger):
        """ Configure logger """

        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
            "<b><level>{level}</level></b> "
            "<level>{message}</level>"
        )
        self.logger = logger
        self.logger.configure(
            handlers=[
                {'sink': sys.stderr, 'format': log_format, 'level': self.log_level},
                {'sink': f'./log/{self.id}.log', 'format': log_format},
                # {'sink': './log/trading.log', 'format': log_format, 'filter': lambda r: r['level'].name == 'TRADING'}
            ],
        )
        try:
            self.logger.level('TRADING', no=60, color='<magenta><bold>')
        except:
            pass
        self.logger.success(json.dumps({self.id: 'Strategy Initialized'}))

    """ Exchange Operation """

    def snap_account_value(self):
        if self.this_ts - self.last_account_snap > self.account_snap_interval:
            self.last_account_snap = self.this_ts
            position = self.account['position']
            tick = self.cache['tick']
            log_dict = {
                'ts': self.this_ts,
                'account_value': self.account['value']
            }
            self.logger.log(
                'TRADING',
                json.dumps(log_dict)
            )

    def set_exchange(self, exchange):
        """ Connect to exchange """
        self.exchange = exchange
        if self.execution:
            self.execution.set_exchange(exchange)

    def get_account(self):
        """ Get current account snap """
        return self.exchange.get_account()

    def get_pending_orders(self):
        """ Get pending orders """
        return self.exchange.get_pending_orders()

    def _log_order(self, order):
        if order.status == OrderStatus.NEW:
            log_dict = {
                'event': 'order',
                'timestamp': order.timestamp,
                'side': order.side.value,
                'quantity': order.quantity,
                'price': order.price,
                'type': order.type.value
            }
            self.logger.log(
                'TRADING',
                json.dumps(log_dict)
            )

    def buy(self, **kwargs):
        """ Send buy order to exchange """
        order = self.exchange.buy(**kwargs)
        self._log_order(order)
        return order

    def short(self, **kwargs):
        """ Send short order to exchange """
        order = self.exchange.short(**kwargs)
        self._log_order(order)
        return order

    def sell(self, **kwargs):
        """ Send sell order to exchange """
        order = self.exchange.sell(**kwargs)
        self._log_order(order)
        return order

    def cover(self, **kwargs):
        """ Send cover order to exchange """
        order = self.exchange.cover(**kwargs)
        self._log_order(order)
        return order

    def cancel(self, **kwargs):
        """ Cancel order by order id """
        order = self.exchange.cancel(**kwargs)
        self._log_order(order)
        return order

    def cancel_all(self):
        """ Cancel all pending orders """
        orders = self.exchange.cancel_all()
        for order in orders:
            self._log_order(order)
        return orders

    def rebalance(self, symbol, now_price, now_position, target_price, target_position):
        """ Send orders to reach exact position at target price level """
        now_long = now_position['long']
        now_short = now_position['short']
        target_long = target_position['long']
        target_short = target_position['short']

        order_ids = set()

        # adjust long position
        long_delta = target_long - now_long
        if long_delta > 0:
            order_id, _ = self.buy(
                symbol=symbol,
                price=target_price,
                quantity=long_delta,
                type='limit' if target_price < now_price else ('stop' if target_price > now_price else 'market'),
            )
            order_ids.add(order_id)
        elif long_delta < 0:
            order_id, _ = self.sell(
                symbol=symbol,
                price=target_price,
                quantity=abs(long_delta),
                type='limit' if target_price > now_price else ('stop' if target_price < now_price else 'market'),
            )
            order_ids.add(order_id)

        # adjust short position
        short_delta = target_short - now_short
        if short_delta > 0:
            order_id, _ = self.short(
                symbol=symbol,
                price=target_price,
                quantity=short_delta,
                type='limit' if target_price > now_price else ('stop' if target_price < now_price else 'market'),
            )
            order_ids.add(order_id)
        elif short_delta < 0:
            order_id, _ = self.cover(
                symbol=symbol,
                price=target_price,
                quantity=abs(short_delta),
                type='limit' if target_price < now_price else ('stop' if target_price > now_price else 'market'),
            )
            order_ids.add(order_id)
        return order_ids


class Strategy:
    """ Deprecated tick level strategy template. Will be merged into minute level strategy later. """

    def __init__(self, symbol, params=None, log_level=0):
        params = params if params is not None else {}

        # switch of logging
        self.verbose = params.get('verbose', True)

        # all symbols
        self.symbol = symbol if isinstance(symbol, list) else [symbol]

        # exchange protocol
        self.exchange = None

        # save account information (capital, shares, etc.,)
        self.account = dict()

        # store latest tick data
        self.this_tick = dict()

        # interval of account value snap
        self.account_snap_interval = 5 * 1000
        self.last_account_snap = 0

        # length of cached ticks (in 0.001 second)
        self.cache_window = 3600 * 1000

        # cache aggregation (bar data) period (in 0.001 second)
        self.cache_bar_period = 60 * 1000
        self.cache_bar_shift = 0

        # dictionary to store all tick data
        self.cache = None

        # store all bar aggregation function
        self.bar_aggregation = []
        self.bar_extra_fields = []

        # store all tick aggregation function
        self.tick_aggregation = []
        self.tick_extra_fields = []

        # log level
        self.log_level = int(log_level)

        # logger to record all intermediate data
        self.id = base36(int(datetime.datetime.now().strftime('%y%m%d%H%M%S')))
        self.config_logger(logger)

    """ Helper Functions """

    def config_logger(self, logger):
        """ Configure logger """

        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
            "<b><level>{level}</level></b> "
            "<level>{message}</level>"
        )
        self.logger = logger
        self.logger.configure(
            handlers=[
                {'sink': sys.stderr, 'format': log_format, 'level': self.log_level},
                {'sink': f'./log/{self.id}.log', 'format': log_format},
                # {'sink': './log/trading.log', 'format': log_format, 'filter': lambda r: r['level'].name == 'TRADING'}
            ],
        )
        try:
            self.logger.level('TRADING', no=60, color='<magenta><bold>')
        except:
            pass
        self.logger.success(json.dumps({self.id: 'Strategy Initialized'}))

    """ Data Manipulation """

    def cache_init(self):
        """
            Init cache data structure

            Cache Structure:

                (Dict) self.cache
                -- (Dict) tick
                -- -- (Dict) $symbol
                -- -- -- (Int) count
                -- -- -- (Deque) timestamp
                -- -- -- (Deque) price
                -- -- -- (Deque) quantity
                -- -- -- (Deque) direction
                -- (Dict) bar
                -- -- (Dict) $symbol
                -- -- -- (Int) count
                -- -- -- (Deque) timestamp
                -- -- -- (Deque) open
                -- -- -- (Deque) close
                -- -- -- (Deque) high
                -- -- -- (Deque) low
                -- -- -- (Deque) vwap
                -- -- -- (Deque) volume
                -- -- -- (Deque) num_trades

        """
        self.cache = dict()

        # init tick data structure
        self.cache['tick'] = dict()
        for sym in self.symbol:
            self.cache['tick'][sym] = dict(
                count=0,
                timestamp=deque(),
                price=deque(),
                quantity=deque(),
                direction=deque(),
            )
            self.cache['tick'][sym].update({
                field: deque() for field in self.tick_extra_fields
            })

        # init bar data structure
        if self.cache_bar_period:
            self.cache_bar_ts = 0
            self.cache_bar_data = dict()
            self.cache['bar'] = dict()
            for sym in self.symbol:
                self.cache['bar'][sym] = dict(
                    count=0,
                    timestamp=deque(),
                    open=deque(),
                    close=deque(),
                    high=deque(),
                    low=deque(),
                    vwap=deque(),
                    volume=deque(),
                    num_trades=deque(),
                )
                self.cache['bar'][sym].update({
                    field: deque() for field in self.bar_extra_fields
                })

        # init cache data
        if hasattr(self.exchange, 'get_recent_klines'):
            for sym in self.symbol:
                # get recent klines
                interval = self.cache_bar_period // (60 * 1000)
                shift = self.cache_bar_shift // (60 * 1000)
                bars = self.exchange.get_recent_klines(sym, interval, shift, self.cache_window // (60 * 1000))
                self.logger.success(f"[{sym}] [INIT] {len(bars)} Bars Loaded")
                self.cache_bar_data = dict()
                for b in bars:

                    # update bar value
                    self.cache['bar'][sym]['count'] += 1
                    for field, value in b.items():
                        self.cache['bar'][sym][field].append(value)

                    # calculate aggregation
                    for aggregate in self.bar_aggregation:
                        aggregate(suppress_log=True)

                    self.cache_bar_data[sym] = deque()
                self.cache_bar_ts = max(self.cache_bar_ts, bars[-1]['timestamp'])

        if hasattr(self.exchange, 'get_recent_trades'):

            # load recent ticks
            self.this_tick = dict()
            for sym in self.symbol:
                self.this_tick[sym] = self.exchange.get_recent_trades(sym)
            self.this_ts = self.exchange.get_ts()
            self.logger.success(f"[{sym}] [INIT] {len(self.this_tick[sym])} Ticks Loaded")

            # cache ticks
            self._cache_tick()

            # cache bars
            if self.cache_bar_period:
                self._cache_bar()

    def _cache_tick(self):
        """ Cache ticks data"""
        # cache all ticks
        cache_start = self.this_ts - self.cache_window
        for sym in self.this_tick:
            for tick in self.this_tick[sym]:
                self.cache['tick'][sym]['count'] += 1
                self.cache['tick'][sym]['timestamp'].append(tick['timestamp'])
                self.cache['tick'][sym]['price'].append(tick['price'])
                self.cache['tick'][sym]['quantity'].append(tick['quantity'])
                self.cache['tick'][sym]['direction'].append(tick['direction'])

        # pop stale data
        for sym in self.symbol:
            while self.cache['tick'][sym]['timestamp'][0] < cache_start:
                self.cache['tick'][sym]['count'] -= 1
                self.cache['tick'][sym]['timestamp'].popleft()
                self.cache['tick'][sym]['price'].popleft()
                self.cache['tick'][sym]['quantity'].popleft()
                self.cache['tick'][sym]['direction'].popleft()

        # calculate extra tick data
        for aggregate in self.tick_aggregation:
            aggregate()

    def _cache_bar(self):
        """ Cache bars data """

        if self.cache_bar_ts == 0:
            # init bar data
            self.cache_bar_ts = self.this_ts // self.cache_bar_period * self.cache_bar_period + self.cache_bar_shift
            self.cache_bar_data = self.this_tick.copy()

        else:
            # update bar data
            if self.this_ts < self.cache_bar_ts + self.cache_bar_period:
                # if no new bar created
                # extend data in the bar
                for sym in self.symbol:
                    self.cache_bar_data[sym].extend(self.this_tick[sym])
            else:
                # if new bar created
                # append data in the bar
                for sym in self.symbol:
                    if not self.this_tick[sym]:
                        continue
                    while self.this_tick[sym][0]['timestamp'] < self.cache_bar_ts + self.cache_bar_period:
                        self.cache_bar_data[sym].append(self.this_tick[sym].popleft())

                # aggregate tick data
                sym_data = {
                    sym: {
                        'price': list(),
                        'quantity': list()
                    }
                    for sym in self.symbol
                }
                for sym in self.symbol:
                    sym_data[sym]['price'] = [x['price'] for x in self.cache_bar_data[sym]]
                    sym_data[sym]['quantity'] = [x['quantity'] for x in self.cache_bar_data[sym]]

                # aggregate bar
                for sym, data in sym_data.items():
                    price_series = np.array(data['price'])
                    quantity_series = np.array(data['quantity'])
                    turnover_series = price_series * quantity_series
                    self.cache['bar'][sym]['count'] += 1
                    self.cache['bar'][sym]['timestamp'].append(self.cache_bar_ts + self.cache_bar_period)
                    self.cache['bar'][sym]['open'].append(price_series[0])
                    self.cache['bar'][sym]['close'].append(price_series[-1])
                    self.cache['bar'][sym]['high'].append(price_series.max())
                    self.cache['bar'][sym]['low'].append(price_series.min())
                    self.cache['bar'][sym]['vwap'].append(turnover_series.sum() / quantity_series.sum())
                    self.cache['bar'][sym]['volume'].append(quantity_series.sum())
                    self.cache['bar'][sym]['num_trades'].append(len(price_series))

                    log_fields = ['open', 'close', 'high', 'low', 'vwap', 'volume', 'num_trades']
                    log_dict = {'ts': self.this_ts, 'bar': {sym: {field: self.cache['bar'][sym][field][-1] for field in log_fields}}}
                    self.logger.info(json.dumps(log_dict))

                    # pop stale data
                    while self.cache['bar'][sym]['timestamp'][0] <= self.cache_bar_ts + self.cache_bar_period - self.cache_window:
                        self.cache['bar'][sym]['count'] -= 1
                        self.cache['bar'][sym]['timestamp'].popleft()
                        self.cache['bar'][sym]['open'].popleft()
                        self.cache['bar'][sym]['close'].popleft()
                        self.cache['bar'][sym]['high'].popleft()
                        self.cache['bar'][sym]['low'].popleft()
                        self.cache['bar'][sym]['vwap'].popleft()
                        self.cache['bar'][sym]['volume'].popleft()
                        self.cache['bar'][sym]['num_trades'].popleft()

                # calculate extra bar data
                for aggregate in self.bar_aggregation:
                    aggregate()

                # record new bar
                self.cache_bar_ts = self.this_ts // self.cache_bar_period * self.cache_bar_period + self.cache_bar_shift
                self.cache_bar_data = self.this_tick.copy()

    def cache_data(self):
        """ Cache ticks data """
        # # clean up memory
        # gc.collect()

        # cache tick data
        self._cache_tick()

        # cache bar data
        if self.cache_bar_period:
            self._cache_bar()

    """ Exchange Operation """

    def snap_account_value(self):
        if self.this_ts - self.last_account_snap > self.account_snap_interval:
            self.last_account_snap = self.this_ts
            position = self.account['position']
            tick = self.cache['tick']
            log_dict = {
                'ts': self.this_ts,
                'account_value': self.account['value']
            }
            self.logger.log(
                'TRADING',
                json.dumps(log_dict)
            )

    def set_exchange(self, exchange):
        """ Connect to exchange """
        self.exchange = exchange

    def get_tick(self):
        """ Get current market tick """
        return self.exchange.get_tick()

    def get_account(self):
        """ Get current account snap """
        return self.exchange.get_account()

    def get_pending_orders(self):
        """ Get pending orders """
        return self.exchange.get_pending_orders()

    def buy(self, **kwargs):
        """ Send buy order to exchange """
        log_dict = {'ts': self.this_ts, 'order': kwargs.copy()}
        log_dict['order'].update({'side': 'buy'})
        self.logger.log(
            'TRADING',
            json.dumps(log_dict)
        )
        return self.exchange.buy(**kwargs)

    def short(self, **kwargs):
        """ Send short order to exchange """
        log_dict = {'ts': self.this_ts, 'order': kwargs.copy()}
        log_dict['order'].update({'side': 'short'})
        self.logger.log(
            'TRADING',
            json.dumps(log_dict)
        )
        return self.exchange.short(**kwargs)

    def sell(self, **kwargs):
        """ Send sell order to exchange """
        log_dict = {'ts': self.this_ts, 'order': kwargs.copy()}
        log_dict['order'].update({'side': 'sell'})
        self.logger.log(
            'TRADING',
            json.dumps(log_dict)
        )
        return self.exchange.sell(**kwargs)

    def cover(self, **kwargs):
        """ Send cover order to exchange """
        log_dict = {'ts': self.this_ts, 'order': kwargs.copy()}
        log_dict['order'].update({'side': 'cover'})
        self.logger.log(
            'TRADING',
            json.dumps(log_dict)
        )
        return self.exchange.cover(**kwargs)

    def cancel(self, **kwargs):
        """ Cancel order by order id """
        log_dict = {'ts': self.this_ts, 'order': kwargs.copy()}
        log_dict['order'].update({'side': 'cancel'})
        self.logger.log(
            'TRADING',
            json.dumps(log_dict)
        )
        return self.exchange.cancel(**kwargs)

    def cancel_all(self):
        """ Cancel all pending orders """
        log_dict = {'ts': self.this_ts, 'order': {'side': 'cancel_all'}}
        self.logger.log(
            'TRADING',
            json.dumps(log_dict)
        )
        return self.exchange.cancel_all()

    def rebalance(self, symbol, now_price, now_position, target_price, target_position):
        """ Send orders to reach exact position at target price level """
        now_long = now_position['long']
        now_short = now_position['short']
        target_long = target_position['long']
        target_short = target_position['short']

        order_ids = set()

        # adjust long position
        long_delta = target_long - now_long
        if long_delta > 0:
            order_id, _ = self.buy(
                symbol=symbol,
                price=target_price,
                quantity=long_delta,
                type='limit' if target_price < now_price else ('stop' if target_price > now_price else 'market'),
            )
            order_ids.add(order_id)
        elif long_delta < 0:
            order_id, _ = self.sell(
                symbol=symbol,
                price=target_price,
                quantity=abs(long_delta),
                type='limit' if target_price > now_price else ('stop' if target_price < now_price else 'market'),
            )
            order_ids.add(order_id)

        # adjust short position
        short_delta = target_short - now_short
        if short_delta > 0:
            order_id, _ = self.short(
                symbol=symbol,
                price=target_price,
                quantity=short_delta,
                type='limit' if target_price > now_price else ('stop' if target_price < now_price else 'market'),
            )
            order_ids.add(order_id)
        elif short_delta < 0:
            order_id, _ = self.cover(
                symbol=symbol,
                price=target_price,
                quantity=abs(short_delta),
                type='limit' if target_price < now_price else ('stop' if target_price > now_price else 'market'),
            )
            order_ids.add(order_id)
        return order_ids

