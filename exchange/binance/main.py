import time
import traceback
from collections import deque
from binance import Client
from exchange.enums import OrderStatus
from config import trading_config

API_KEY = trading_config['api_key']
SECRET_KEY = trading_config['secret_key']


class BinanceProtocol:
    """ Deprecated binance exchange API. Will be merged into websocket API later. """

    def __init__(self, symbol, params=None):

        self.symbol = symbol if isinstance(symbol, list) else [symbol]
        self.tick_timestamp = 0
        self.tick_stream = deque()
        self.tick_id = {sym: 0 for sym in self.symbol}
        self.this_ts = 9
        self.client = Client(API_KEY, SECRET_KEY)
        self.client.futures_change_leverage(symbol=self.symbol[0], leverage=5)

    def _send_order(self, **kwargs):
        if abs(kwargs['quantity']) > 0:
            self.client.futures_create_order(**kwargs)
            return 0, OrderStatus.Received
        return None, OrderStatus.Failed

    def buy(self, symbol, price, quantity, type='market', callback=None, tag=''):
        param_dict = dict(
            symbol=symbol,
            side='BUY',
            positionSide='LONG',
            quantity=quantity,
            type='LIMIT' if type.upper() == 'LIMIT' else 'MARKET',
        )
        param_dict.update({'type': 'LIMIT', 'price': price} if type.upper() == 'LIMIT' else {'type': 'MARKET'})
        return self._send_order(**param_dict)

    def sell(self, symbol, price, quantity, type='market', callback=None, tag=''):
        param_dict = dict(
            symbol=symbol,
            side='SELL',
            positionSide='LONG',
            quantity=quantity,
            type='LIMIT' if type.upper() == 'LIMIT' else 'MARKET',
        )
        param_dict.update({'type': 'LIMIT', 'price': price} if type.upper() == 'LIMIT' else {'type': 'MARKET'})
        return self._send_order(**param_dict)

    def short(self, symbol, price, quantity, type='market', callback=None, tag=''):
        param_dict = dict(
            symbol=symbol,
            side='SELL',
            positionSide='SHORT',
            quantity=quantity,
            type='LIMIT' if type.upper() == 'LIMIT' else 'MARKET',
        )
        param_dict.update({'type': 'LIMIT', 'price': price} if type.upper() == 'LIMIT' else {'type': 'MARKET'})
        return self._send_order(**param_dict)

    def cover(self, symbol, price, quantity, type='market', callback=None, tag=''):
        param_dict = dict(
            symbol=symbol,
            side='BUY',
            positionSide='SHORT',
            quantity=quantity,
            type='LIMIT' if type.upper() == 'LIMIT' else 'MARKET',
        )
        param_dict.update({'type': 'LIMIT', 'price': price} if type.upper() == 'LIMIT' else {'type': 'MARKET'})
        return self._send_order(**param_dict)

    def get_account(self):
        """ Get account information """

        # query account info
        res = self.client.futures_account()

        # parse account value
        value = float(res['totalWalletBalance']) + float(res['totalUnrealizedProfit'])

        # parse cash data
        cash = float(res['availableBalance'])

        # parse position information
        position = {sym: dict() for sym in self.symbol}
        for p in res['positions']:
            sym = p['symbol']
            if sym in position:
                position[sym][p['positionSide'].lower()] = abs(float(p['positionAmt']))

        return dict(
            value=value,
            cash=cash,
            position=position
        )

    def get_ts(self):
        return self.this_ts

    def get_tick(self):
        this_tick = dict()
        for sym in self.symbol:
            while True:
                try:
                    if self.tick_id[sym] == 0:
                        res = self.client.futures_recent_trades(symbol=sym)
                        self.tick_id[sym] = res[-1]['id']
                        self.this_ts = res[-1]['time']
                    else:
                        res = self.client.futures_historical_trades(symbol=sym, limit=1000, fromId=self.tick_id[sym]+1)
                        self.tick_id[sym] = max(self.tick_id[sym], 0 if not res else res[-1]['id'])
                        self.this_ts = max(self.this_ts, 0 if not res else res[-1]['time'])
                    break
                except:
                    traceback.print_exc()
                    self.client = Client(API_KEY, SECRET_KEY)
            res = deque({
                'symbol': sym,
                'timestamp': x['time'],
                'price': float(x['price']),
                'quantity': float(x['qty']),
                'direction': 1 if x['isBuyerMaker'] else -1,
            } for x in res)
            this_tick[sym] = res
        return this_tick

    def get_recent_klines(self, symbol: str, interval: int = 1, shift: int = 0, limit: int = 1440):
        """ Get heat up klines """
        raw_bars = self.client.futures_historical_klines(
            symbol, Client.KLINE_INTERVAL_1MINUTE, f'{limit + 1} minute ago UTC'
        )

        # reformat bar data
        bar_data = list()
        tmp_bar = [list() for _ in range(11)]
        for b in raw_bars[:-1]:
            for i in range(11):
                tmp_bar[i].append(float(b[i]))

            if (b[0] + 60 * 1000) % (interval * 60 * 1000) == shift * 60 * 1000:
                bar_data.append(dict(
                    timestamp=b[0] + 60 * 1000,
                    open=tmp_bar[1][0],
                    high=max(tmp_bar[2]),
                    low=min(tmp_bar[3]),
                    close=tmp_bar[4][-1],
                    volume=sum(tmp_bar[5]),
                    vwap=sum(tmp_bar[7]) / sum(tmp_bar[5]),
                    num_trades=sum(tmp_bar[8]),
                ))
                tmp_bar = [list() for _ in range(11)]
        self.this_ts = max(self.this_ts, bar_data[-1]['timestamp'])
        return bar_data

    def get_recent_trades(self, symbol: str):
        """ Get heat up trades """
        raw_trades = deque(self.client.futures_historical_trades(symbol=symbol, limit=1000))

        # iteratively query until covers the latest timestamp
        while raw_trades[0]['time'] > self.this_ts:
            raw_trades.extendleft(self.client.futures_historical_trades(
                symbol=symbol, limit=1000, fromId=raw_trades[0]['id'] - 1000
            )[::-1])

        # pop out stale ticks
        while raw_trades[0]['time'] <= self.this_ts:
            raw_trades.popleft()

        # reformat ticks
        recent_trades = deque({
            'symbol': symbol,
            'timestamp': x['time'],
            'price': float(x['price']),
            'quantity': float(x['qty']),
            'direction': 1 if x['isBuyerMaker'] else -1,
        } for x in raw_trades)
        self.this_ts = max(self.this_ts, recent_trades[-1]['timestamp'])
        self.tick_id[symbol] = max(self.tick_id[symbol], raw_trades[-1]['id'])

        return recent_trades

    def run(self, strategy, interval=200):
        # self.init_exchange(interval)
        strategy.set_exchange(self)
        strategy.cache_init()
        while True:
            time.sleep(interval / 1000.)
            strategy.on_tick()

