import time
import urllib
import json
import numpy as np
import pandas as pd

from collections import deque
from exchange.enums import Order, OrderStatus, OrderSide, OrderType
from copy import deepcopy


def get_precision():
    url = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
    response = urllib.request.urlopen(url).read()
    response = json.loads(response)['symbols']
    precision = dict()
    for r in response:
        precision[r['symbol']] = {
            'price_precision': r['pricePrecision'],
            'quantity_precision': r['quantityPrecision'],
        }
    return precision

class MinuteSimulator:
    """ Minute level simulator """

    params = dict(
        cost_taker=0.0004,
        cost_maker=0.0002,
        slippage=0.0,
        init_cash=10000,
    )

    def __init__(self, params=None):
        """ Minute Replay Exchange """
        self.params.update(params if isinstance(params, dict) else dict())
        self.symbol = list()
        self.cost_taker = self.params.get('cost_taker', 0.0004)
        self.cost_maker = self.params.get('cost_maker', 0.0002)
        self.slippage = self.params.get('slippage', 0)
        self.bar_data = dict()
        self.stream_order_callback = None
        self.timestamp_now = 0
        self.precision = get_precision()

    def load_local(self, data):
        """ Load bar data into memory """
        self.bar_data = data[['symbol', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'count']]
        self.bar_data = pd.concat([self.bar_data, (data.datetime - pd.Timestamp("1970-01-01")) // pd.Timedelta('1ms')], axis=1)
        self.bar_data = self.bar_data.rename(columns={'datetime': 'timestamp'})
        self.bar_data = self.bar_data.sort_values('timestamp', ascending=False)
        self.symbol = data['symbol'].unique().tolist()

    def init_exchange(self):
        """ Function to init exchange """
        self.cash = self.params['init_cash']
        self.position = {sym: 0 for sym in self.symbol}
        self.next_order_id = 1
        self.pending_orders = {}
        self.history_orders = {}
        self.latest_price = {sym: np.nan for sym in self.symbol}

    def run(self, strategy):
        """ Function to playback order flow """
        self.run_start_time = time.time()
        self.symbol = strategy.symbol
        self.init_exchange()
        # strategy.cache_init()
        strategy.set_exchange(self)
        # self.order_callback = strategy.stream_order_callback

        self.stream_bar(callback=strategy.stream_kline_callback)

        print('Exchange running {:.4f} seconds'.format(time.time() - self.run_start_time))

    def stream_bar(self, callback):
        """ Stream bar data to strategy """
        self.bar_sequence = self.bar_data[self.bar_data.symbol.isin(self.symbol)].to_dict('record')
        while self.bar_sequence:
            bar_data = self.bar_sequence.pop()
            self.timestamp_now = bar_data['timestamp']
            self.latest_price[bar_data['symbol']] = bar_data['close']
            self._match_orders(bar_data)
            callback(bar_data)

    def get_account(self):
        value = sum([self.position[sym] * self.latest_price[sym] for sym in self.symbol])
        value = value + self.cash
        return dict(
            cash=self.cash,
            position=self.position,
            value=value,
        )

    def get_precision(self):
        return self.precision

    """ Order Management """

    def buy(self, symbol, price, quantity, type='market', custom_id=None):
        """ Function to send buy order """
        return self._send_order(symbol, 'BUY', price, quantity, type, custom_id)

    def sell(self, symbol, price, quantity, type='market', custom_id=None):
        """ Function to send sell order """
        return self._send_order(symbol, 'SELL', price, quantity, type, custom_id)

    def cancel(self, order_id=None, custom_id=None, callback=None):
        """ Function to cancel order """
        if order_id:
            order = self.pending_orders.pop(order_id, None)
            if order:
                order.status = OrderStatus.CANCELED
                self.history_orders.update({order_id: order})
                if self.stream_order_callback:
                    self.stream_order_callback(order)
            else:
                order = Order(error=-2011)
        elif custom_id:
            order = [o for o in self.pending_orders.values() if o.custom_id == custom_id]
            if order:
                order = order[0]
                self.pending_orders.pop(order.id, None)
                order.status = OrderStatus.CANCELED
                self.history_orders.update({order.id: order})
                if self.stream_order_callback:
                    self.stream_order_callback(order)
            else:
                order = Order(error=-2011)
        else:
            order = Order(error=-2011)
        return order

    def cancel_all(self):
        """ Function to cancel all pending orders """
        orders = []
        for order_id, order in self.pending_orders.items():
            order.status = OrderStatus.CANCELED
            self.history_orders.update({order_id: order})
            orders.append(order)
            if self.stream_order_callback:
                self.stream_order_callback(order)
        self.pending_orders = {}
        return orders

    def _send_order(self, symbol, side, price, quantity, type, custom_id):
        """ Function to send order """
        if quantity > 0:
            order_id = self.next_order_id
            self.next_order_id += 1
            order = Order(
                id=order_id,
                custom_id=custom_id,
                timestamp=self.timestamp_now,
                symbol=symbol,
                price=price,
                quantity=quantity,
                side=side,
                type=type,
                status='NEW'
            )
            self.pending_orders.update({order_id: order})
            return order
        return Order(status='CANCELED')

    def _fill_buy(self, symbol, price, order, cost):
        """ Function to fill buy order """
        order.status = OrderStatus.FILLED
        order.executed_quantity = order.quantity
        self.cash -= order.executed_quantity * price * (1 + cost)
        self.position[symbol] += order.executed_quantity
        self.pending_orders.pop(order.id)
        self.history_orders.update({order.id: order})
        if self.stream_order_callback:
            self.stream_order_callback(order)

    def _fill_sell(self, symbol: str, price: float, order: Order, cost: float):
        """ Function to fill sell order """
        order.status = OrderStatus.FILLED
        order.executed_quantity = order.quantity
        self.cash += order.executed_quantity * price * (1 - cost)
        self.position[symbol] -= order.executed_quantity
        self.pending_orders.pop(order.id)
        self.history_orders.update({order.id: order})
        if self.stream_order_callback:
            self.stream_order_callback(order)

    def _match_orders(self, bar_data):
        """ Function to match pending orders """
        order_ids = list(self.pending_orders.keys())
        for order_id in order_ids:
            order = self.pending_orders.get(order_id)
            symbol = order.symbol
            if symbol != bar_data['symbol']:
                continue

            # match market order
            if order.type == OrderType.MARKET:
                price = bar_data['open']
                if order.side == OrderSide.BUY:
                    self._fill_buy(symbol, price + self.slippage, order, self.cost_taker)
                elif order.side == OrderSide.SELL:
                    self._fill_sell(symbol, price - self.slippage, order, self.cost_taker)

            # match limit order
            elif order.type == OrderType.LIMIT:
                if bar_data['high'] >= order.price >= bar_data['low']:
                    if order.side == OrderSide.BUY:
                        self._fill_buy(symbol, order.price, order, self.cost_maker)
                    elif order.side == OrderSide.SELL:
                        self._fill_sell(symbol, order.price, order, self.cost_maker)

            # match stop order
            elif order.type == OrderType.STOP:
                if not price:
                    continue
                if (order.side == OrderSide.BUY) and (bar_data['high'] >= order.price):
                    self._fill_buy(symbol, order.price, order, self.cost_taker)
                elif (order.side == OrderSide.SELL) and (bar_data['low'] <= order.price):
                    self._fill_sell(symbol, order.price, order, self.cost_taker)

if __name__ == '__main__':

    data = pd.read_pickle('test.pkl')
    min_sim = MinuteSimulator()
    min_sim.load_local(data)
    # min_sim.stream_bar(lambda x: print(x))



