import time
import heapq
import numpy as np

from collections import deque
from exchange.enums import OrderStatus
from copy import deepcopy

class ExchangeSimulator:
    """ Tick level simulator """

    params = dict(
        cost_taker=0.0004,
        cost_maker=0.0002,
        slippage=0.0,
        full_fill=True,  # fill order no matter the amount of trades
        two_sided=False,  # allow long/short orders exist at the same time
        init_cash=10000,
        allow_outlot=True,  # allow order less than 1 contract
        group_tick=True
    )

    def __init__(self, params=None):
        """ One Symbol Exchange """
        self.params.update(params if isinstance(params, dict) else {})
        self.symbol = list()
        self.cost_taker = self.params.get('cost_taker', 0)
        self.cost_maker = self.params.get('cost_maker', 0)
        self.slippage = self.params.get('slippage', 0)
        self.tick_data = dict()
        self.tick_sequence = list()

    def load_local(self, symbol, data):
        """ Load data from local dataframe """
        data['symbol'] = symbol
        self.tick_data.update({symbol: data.to_dict(orient='records')})
        self.symbol.append(symbol.upper())

    """ Main Functions """

    def init_exchange(self, interval=1):
        """ Function to init exchange """
        self.cash = self.params['init_cash']
        self.position = {sym: {'long': 0, 'short': 0} for sym in self.symbol}
        self.next_order_id = 1
        self.pending_orders = {}
        self.history_orders = {}

        # merge all symbols into one stream
        self.tick_template = {sym: deque() for sym in self.symbol}
        self.tick_sequence = deque(heapq.merge(*self.tick_data.values(), key=lambda x: x['timestamp']))
        self.tick_count = len(self.tick_sequence)
        self.status_print_size = max(self.tick_count // 20, 1)
        self.interval = interval
        self.latest_price = {sym: np.nan for sym in self.symbol}
        for _ in range(5):
            self.next_tick()

    def run(self, strategy, interval=1):
        """ Function to playback order flow """
        self.run_start_time = time.time()
        self.init_exchange(interval)
        strategy.cache_init()
        strategy.set_exchange(self)
        while self.next_tick():
            strategy.on_tick()
        print('Exchange running {:.4f} seconds'.format(time.time() - self.run_start_time))

    """ Tick Functions """

    def next_tick(self):
        """ Function to move to next tick """
        # check if any tick left
        if self.tick_count <= 0:
            return False

        # clear tick data
        self.this_tick = deepcopy(self.tick_template)

        # tick by tick mode
        if self.interval == 1:
            self.pop_tick()

        # fixed time interval mode
        else:
            tick_end = self.tick_sequence[0]['timestamp'] // self.interval * self.interval + self.interval - 1
            while (self.tick_count > 0) and (self.tick_sequence[0]['timestamp'] <= tick_end):
                self.pop_tick()

        return True

    def pop_tick(self):
        """ Process each tick """

        # set next timestamp
        self.this_ts = self.tick_sequence[0]['timestamp']

        # pop ticks at this timestamp
        while self.tick_sequence and self.tick_sequence[0]['timestamp'] == self.this_ts:
            tick = self.tick_sequence.popleft()
            self.this_tick[tick['symbol']].append(tick)
            self.latest_price[tick['symbol']] = tick['price']

            # match orders with current tick data
            self._match_orders(tick)

            # decrease count
            self.tick_count -= 1

            # print progress
            if self.tick_count % self.status_print_size == 0:
                processed_pct = 1 - self.tick_count // self.status_print_size * 0.05
                remain_time = (time.time() - self.run_start_time) / processed_pct * (1 - processed_pct) if processed_pct > 0 else -1
                print('[ExchangeSimulator] Progress - {:.0%}  ||  Estimate Time Left - {:.2f} seconds'.format(processed_pct, remain_time))

    """ Helper Functions """

    def get_account(self):
        value = sum([(self.position[sym]['long'] - self.position[sym]['short']) * self.latest_price[sym] for sym in self.symbol])
        value = value + self.cash
        return dict(
            cash=self.cash,
            position=self.position,
            value=value,
        )

    def get_ts(self):
        return self.this_ts

    def get_tick(self):
        return self.this_tick

    """ Trading Functions """

    def buy(self, symbol, price, quantity, type='market', callback=None, tag=''):
        """ Function to send buy order """
        return self._send_order(symbol, 'buy', price, quantity, type, callback, tag)

    def sell(self, symbol, price, quantity, type='market', callback=None, tag=''):
        """ Function to send sell order """
        return self._send_order(symbol, 'sell', price, quantity, type, callback, tag)

    def short(self, symbol, price, quantity, type='market', callback=None, tag=''):
        """ Function to send short order """
        return self._send_order(symbol, 'short', price, quantity, type, callback, tag)

    def cover(self, symbol, price, quantity, type='market', callback=None, tag=''):
        """ Function to send cover order """
        return self._send_order(symbol, 'cover', price, quantity, type, callback, tag)

    def cancel(self, orders_id, callback=None):
        """ Function to cancel order """
        for order_id in orders_id:
            order = self.pending_orders.pop(order_id, None)
            if order:
                order.update({'status': OrderStatus.Cancelled})
                self.history_orders.update({order_id: order})
                if callback:
                    callback(order)
        return orders_id, OrderStatus.Cancelled

    def cancel_all(self, callback=None):
        """ Function to cancel all pending orders """
        order_res = dict()
        for order_id, order in self.pending_orders.items():
            order.update({'status': OrderStatus.Cancelled})
            self.history_orders.update({order_id: order})
            order_res.update({order_id: OrderStatus.Cancelled})
            if callback:
                callback(order)
        self.pending_orders = {}
        return order_res

    def get_pending_orders(self):
        """ Function to get pending order status """
        return self.pending_orders

    """ Orders Management """

    def _send_order(self, symbol, direction, price, quantity, type, callback, tag):
        """ Function to send order """
        if abs(quantity) > 0:
            order_id = self.next_order_id
            self.next_order_id += 1
            self.pending_orders.update({
                order_id: dict(
                    id=order_id,
                    symbol=symbol,
                    price=price,
                    quantity=quantity,
                    direction=direction,
                    type=type,
                    status=OrderStatus.Received,
                    callback=callback,
                    tag=tag,
                )
            })
            return order_id, OrderStatus.Received
        return None, OrderStatus.Failed

    def _fill_buy(self, symbol, price, order, cost):
        """ Function to fill buy order """
        order.update({'status': OrderStatus.Filled})
        self.cash -= order.get('quantity') * price * (1 + cost)
        self.position[symbol]['long'] += order.get('quantity')
        self.pending_orders.pop(order['id'])
        self.history_orders.update({order['id']: order})
        if order.get('callback'):
            order.get('callback')(order)

    def _fill_sell(self, symbol, price, order, cost):
        """ Function to fill sell order """
        order.update({'status': OrderStatus.Filled})
        quantity = min(order.get('quantity'), self.position[symbol]['long'])
        self.cash += quantity * price * (1 - cost)
        self.position[symbol]['long'] -= quantity
        self.pending_orders.pop(order['id'])
        self.history_orders.update({order['id']: order})
        if order.get('callback'):
            order.get('callback')(order)

    def _fill_short(self, symbol, price, order, cost):
        """ Function to fill short order """
        order.update({'status': OrderStatus.Filled})
        self.cash += order.get('quantity') * price * (1 - cost)
        self.position[symbol]['short'] += order.get('quantity')
        self.pending_orders.pop(order['id'])
        self.history_orders.update({order['id']: order})
        if order.get('callback'):
            order.get('callback')(order)

    def _fill_cover(self, symbol, price, order, cost):
        """ Function to fill cover order """
        order.update({'status': OrderStatus.Filled})
        quantity = min(order.get('quantity'), self.position[symbol]['short'])
        self.cash -= quantity * price * (1 + cost)
        self.position[symbol]['short'] -= quantity
        self.pending_orders.pop(order['id'])
        self.history_orders.update({order['id']: order})
        if order.get('callback'):
            order.get('callback')(order)

    def _match_orders(self, tick):
        """ Function to match pending orders """
        order_ids = list(self.pending_orders.keys())
        for order_id in order_ids:
            order = self.pending_orders.get(order_id)
            symbol = order['symbol']
            direction = order['direction']
            order_price = order['price']
            price = tick['price'] if tick['symbol'] == symbol else None

            # match market order
            if order.get('type') == 'market':
                price = price or self.latest_price[symbol]
                if direction == 'buy':
                    self._fill_buy(symbol, price + self.slippage, order, self.cost_taker)
                elif direction == 'sell':
                    self._fill_sell(symbol, price - self.slippage, order, self.cost_taker)
                elif direction == 'short':
                    self._fill_short(symbol, price - self.slippage, order, self.cost_taker)
                elif direction == 'cover':
                    self._fill_cover(symbol, price + self.slippage, order, self.cost_taker)

            # match limit order
            elif order.get('type') == 'limit':
                if not price:
                    continue
                if (direction == 'buy') and (price <= order_price):
                    self._fill_buy(symbol, price, order, self.cost_maker)
                elif (direction == 'sell') and (price >= order_price):
                    self._fill_sell(symbol, price, order, self.cost_maker)
                elif (direction == 'short') and (price >= order_price):
                    self._fill_short(symbol, price, order, self.cost_maker)
                elif (direction == 'cover') and (price <= order_price):
                    self._fill_cover(symbol, price, order, self.cost_maker)

            # match stop order
            elif order.get('type') == 'stop':
                if not price:
                    continue
                if (direction == 'buy') and (price >= order_price):
                    self._fill_buy(symbol, price, order, self.cost_taker)
                elif (direction == 'sell') and (price <= order_price):
                    self._fill_sell(symbol, price, order, self.cost_taker)
                elif (direction == 'short') and (price <= order_price):
                    self._fill_short(symbol, price, order, self.cost_taker)
                elif (direction == 'cover') and (price >= order_price):
                    self._fill_cover(symbol, price, order, self.cost_taker)
