import datetime
import time
import asyncio
import traceback
import urllib
import json
import numpy as np
from binance import Client, ThreadedWebsocketManager
from collections import deque
from exchange.enums import Order, BarData
from utils.notification import notify
from config import trading_config

API_KEY = trading_config['api_key']
SECRET_KEY = trading_config['secret_key']


def get_precision():
    """ Get order precision """
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


class BinanceAccess:
    params = {
        'refresh_interval': 43200,
        'refresh_seconds': 20,
        'aggregate_kline': True,
    }

    def __init__(self, params=None):

        self.symbol = []
        self.params.update({} if params is None else params)
        self.refresh_interval = self.params.get('refresh_interval', 43200)
        self.refresh_seconds = self.params.get('refresh_seconds', 20)
        self.aggregate_kline = self.params.get('aggregate_kline', True)

        # binance connection
        self.client = Client(api_key=API_KEY, api_secret=SECRET_KEY)
        self.twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=SECRET_KEY)

        # configure trading account
        try:
            self.client.futures_change_position_mode(dualSidePosition=False)
            self.client.futures_change_multi_assets_mode(multiAssetsMargin=True)
            for sym in self.symbol:
                self.client.futures_change_leverage(symbol=sym, leverage=5)
        except:
            pass

        # order precision
        self.precision = get_precision()

        # event loop for all async functions
        self.loop = asyncio.new_event_loop()

    async def auto_refresh(self):
        """ Async connection guard """
        # create new connection
        self.reconnect()

        # sleep
        r = await asyncio.sleep(self.refresh_interval)

        # sleep until next mid minute
        now = time.time()
        r = await asyncio.sleep(((now - self.refresh_seconds) // 60 + 1) * 60 + self.refresh_seconds - now)

        print('refresh now')
        print(datetime.datetime.now())

    async def check_aggregation(self):
        """ Bar aggregation check """
        while True:
            r = await asyncio.sleep(0.1)
            for symbol in self.symbol:

                # websocket subscription lag is between [0, 250] ms
                # if no new data within first 250ms, assume bar is completed
                if time.time() * 1000 - self.aggregation[symbol]['timestamp'] > 250:
                    self.force_aggregation(symbol)

    def reconnect(self):
        """ Connect to Binance """
        self.twm.stop()
        self.client = Client(api_key=API_KEY, api_secret=SECRET_KEY)
        self.twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=SECRET_KEY)
        self.twm.start()
        self.stream_order()
        if self.aggregate_kline:
            self.stream_agg_trade()
        else:
            self.stream_kline()

    def stream_agg_trade(self):
        """ Start aggTrade stream """
        streams = [f'{sym.lower()}@aggTrade' for sym in self.symbol]
        self.twm.start_futures_multiplex_socket(callback=self.agg_trade_callback, streams=streams)

    def stream_kline(self):
        """ Start kline stream """
        streams = [f'{sym.lower()}_perpetual@continuousKline_1m' for sym in self.symbol]
        self.twm.start_futures_multiplex_socket(callback=self.kline_callback, streams=streams)

    def stream_order(self):
        """ Start order stream """
        self.twm.start_futures_socket(callback=self.order_callback)

    def _send_order(self, **kwargs):
        """ Send order to exchange """
        if abs(kwargs['quantity']) > 0:
            recv = self.client.futures_create_order(**kwargs)
            order = Order(
                id=recv['orderId'],
                custom_id=recv['clientOrderId'],
                timestamp=recv['updateTime'],
                symbol=recv['symbol'],
                side=recv['side'],
                price=recv['price'],
                quantity=recv['origQty'],
                status=recv['status'],
                type=recv['type']
            )
            return order
        return Order(error=-2011)

    def get_precision(self):
        """ Get exchange precision """
        return self.precision

    def buy(self, symbol, price, quantity, type='market', custom_id=None):
        """ Build params set for buy order """
        param_dict = dict(
            symbol=symbol,
            side='BUY',
            quantity=quantity,
        )
        param_dict.update({'newClientOrderId': custom_id} if custom_id else {})
        param_dict.update({'type': 'LIMIT', 'price': price} if type.upper() == 'LIMIT' else {'type': 'MARKET'})
        return self._send_order(**param_dict)

    def sell(self, symbol, price, quantity, type='market', custom_id=None):
        """ Build params set for sell order """
        param_dict = dict(
            symbol=symbol,
            side='SELL',
            quantity=quantity,
        )
        param_dict.update({'newClientOrderId': custom_id} if custom_id else {})
        param_dict.update({'type': 'LIMIT', 'price': price} if type.upper() == 'LIMIT' else {'type': 'MARKET'})
        return self._send_order(**param_dict)

    def place_batch(self, orders):
        """ Place batch orders """
        batch = list()
        for order in orders:
            quantity_precision = self.precision[order.symbol]['quantity_precision']
            price_precision = self.precision[order.symbol]['price_precision']
            param_dict = dict(
                symbol=order.symbol,
                side=order.side.value,
                type=order.type.value,
                quantity=f'{round(abs(order.quantity), quantity_precision):.{quantity_precision}f}'
            )
            param_dict.update(
                {
                    'price': f'{round(abs(order.price), price_precision):.{price_precision}f}',
                    'timeInForce': 'GTC',
                } if param_dict['type'] == 'LIMIT' else {}
            )
            param_dict.update({'newClientOrderId': order.custom_id} if order.custom_id else {})
            batch.append(param_dict)
        recvs = self.client.futures_place_batch_order(batchOrders=batch)
        print(recvs)
        recv_orders = list()
        for recv in recvs:
            recv_orders.append(Order(
                id=recv['orderId'],
                custom_id=recv['clientOrderId'],
                timestamp=recv['updateTime'],
                symbol=recv['symbol'],
                side=recv['side'],
                price=recv['price'],
                quantity=recv['origQty'],
                status=recv['status'],
                type=recv['type']
            ))
        return recv_orders

    def cancel(self, symbol, order_id=None, custom_id=None):
        """ Cancel order """
        try:
            if order_id:
                recv = self.client.futures_cancel_order(symbol=symbol, orderId=order_id)
            elif custom_id:
                recv = self.client.futures_cancel_order(symbol=symbol, origClientOrderId=custom_id)

            order = Order(
                id=recv['orderId'],
                custom_id=recv['clientOrderId'],
                timestamp=recv['updateTime'],
                symbol=recv['symbol'],
                side=recv['side'],
                price=recv['price'],
                quantity=recv['origQty'],
                executed_quantity=recv['executedQty'],
                status=recv['status'],
                type=recv['type']
            )
        # except BinanceAPIException as e:
        #     order = Order(error=e.code)
        except:
            order = Order(error=-2011)
        return order

    def cancel_batch(self, symbol, order_ids=None, custom_ids=None):
        """ Cancel batch orders """
        try:
            if order_ids:
                recvs = self.client.futures_cancel_orders(symbol=symbol, orderIdList=order_ids)
            elif custom_ids:
                recvs = self.client.futures_cancel_order(symbol=symbol, origClientOrderIdList=custom_ids)
            orders = list()
            for recv in recvs:
                orders.append(Order(
                    id=recv['orderId'],
                    custom_id=recv['clientOrderId'],
                    timestamp=recv['updateTime'],
                    symbol=recv['symbol'],
                    side=recv['side'],
                    price=recv['price'],
                    quantity=recv['origQty'],
                    executed_quantity=recv['executedQty'],
                    status=recv['status'],
                    type=recv['type']
                ))
        # except BinanceAPIException as e:
        #     orders = [Order(error=e.code)]
        except:
            orders = [Order(error=-2011)]
        return orders

    def cancel_all(self):
        """ Cancel all existing orders """
        for sym in self.symbol:
            res = self.client.futures_cancel_all_open_orders(symbol=sym)
        return res

    def get_account(self):
        """ Get account information """

        # query account info
        res = self.client.futures_account()

        # parse account value
        value = float(res['totalWalletBalance']) + float(res['totalUnrealizedProfit'])

        # parse cash data
        cash = float(res['availableBalance'])

        # parse position information
        position = {sym: 0 for sym in self.symbol}
        for p in res['positions']:
            sym = p['symbol']
            if sym in position:
                position[sym] = float(p['positionAmt'])

        return dict(
            value=value,
            cash=cash,
            position=position
        )

    def get_recent_agg_trade(self, symbol, start_ts=None, end_ts=None, from_id=None):
        """ Get recent agg trades """
        if from_id is not None:
            agg_trade = self.client.futures_aggregate_trades(symbol=symbol, fromId=from_id)
        elif start_ts is not None and end_ts is not None:
            agg_trade = self.client.futures_aggregate_trades(symbol=symbol, startTime=start_ts, endTime=end_ts)
        return agg_trade

    def get_recent_klines(self, symbol: str, limit: int = 1440):
        """ Get heat up klines """
        raw_bars = self.client.futures_historical_klines(
            symbol, Client.KLINE_INTERVAL_1MINUTE, f'{limit + 1} minute ago UTC'
        )

        # reformat bar data
        bar_data = list()
        for b in raw_bars[:-1]:
            bar_data.append(dict(
                symbol=symbol,
                timestamp=b[0] + 60 * 1000,
                open=float(b[1]),
                high=float(b[2]),
                low=float(b[3]),
                close=float(b[4]),
                volume=float(b[5]),
                turnover=float(b[7]),
                count=b[8],
            ))

        # populate agg trades for bar aggregation
        agg_trades = self.get_recent_agg_trade(symbol, bar_data[-1]['timestamp'], bar_data[-1]['timestamp'] + 60 * 1000 - 1)
        aggregation = self.aggregation[symbol]
        aggregation['timestamp'] = bar_data[-1]['timestamp'] + 60 * 1000
        aggregation['now'] = agg_trades[-1]['T']
        aggregation['trade_id'] = agg_trades[-1]['l']
        aggregation['agg_id'] = agg_trades[-1]['a']
        for trade in agg_trades:
            aggregation['price'].append(float(trade['p']))
            aggregation['volume'].append(float(trade['q']))
        return bar_data

    def force_aggregation(self, symbol):
        """ Force aggregate to new bar if no new trade comes in """
        aggregation = self.aggregation[symbol]
        price = np.array(aggregation['price'])
        volume = np.array(aggregation['volume'])
        bar = BarData(
            symbol=symbol,
            timestamp=aggregation['timestamp'],
            open=price[0],
            high=price.max(),
            low=price.min(),
            close=price[-1],
            volume=volume.sum(),
            turnover=(price * volume).sum(),
            count=len(price)
        )
        aggregation['timestamp'] += 60 * 1000
        aggregation['price'] = deque()
        aggregation['volume'] = deque()
        for callback in self.stream_kline_callback:
            callback(bar)

    def aggregate_1m(self, symbol, agg_trades):
        """ Aggregate trades data to 1 minute bar """
        aggregation = self.aggregation[symbol]
        bar = None
        for trade in agg_trades:
            aggregation['trade_id'] = trade['l']
            aggregation['agg_id'] = trade['a']

            # if still in the same bar
            if trade['T'] < aggregation['timestamp']:
                aggregation['now'] = trade['T']
                aggregation['price'].append(float(trade['p']))
                aggregation['volume'].append(float(trade['q']))

            # if in a new bar
            else:
                price = np.array(aggregation['price'])
                volume = np.array(aggregation['volume'])
                bar = BarData(
                    symbol=symbol,
                    timestamp=aggregation['timestamp'],
                    open=price[0],
                    high=price.max(),
                    low=price.min(),
                    close=price[-1],
                    volume=volume.sum(),
                    turnover=(price * volume).sum(),
                    count=len(price)
                )
                aggregation['timestamp'] += 60 * 1000
                aggregation['price'] = deque([float(trade['p'])])
                aggregation['volume'] = deque([float(trade['q'])])

        return bar

    def agg_trade_callback(self, message):
        """ Wrap Binance agg_trade message to standard format """
        # print(message)
        # print(time.time() * 1000 - message['data']['T'])
        payload = message['data']
        symbol = payload['s']
        aggregation = self.aggregation[symbol]
        if payload['a'] - aggregation['agg_id'] > 1:
            agg_trades = self.get_recent_agg_trade(symbol, from_id=aggregation['agg_id'] + 1)
            if payload['a'] > agg_trades[-1]['a']:
                agg_trades.append(payload)
        else:
            agg_trades = [payload]
        bar = self.aggregate_1m(symbol, agg_trades)
        if bar:
            for callback in self.stream_kline_callback:
                # print('trade callback')
                callback(bar)

    def kline_callback(self, message):
        """ Wrap Binance message to standard format """

        if message['data']['k']['x']:
            bar_data = BarData(
                symbol=message['data']['ps'],
                timestamp=message['data']['k']['t'] + 60 * 1000,
                open=float(message['data']['k']['o']),
                high=float(message['data']['k']['h']),
                low=float(message['data']['k']['l']),
                close=float(message['data']['k']['c']),
                volume=float(message['data']['k']['v']),
                turnover=float(message['data']['k']['q']),
                count=message['data']['k']['n'],
            )
            try:
                for callback in self.stream_kline_callback:
                    # print('kline callback')
                    callback(bar_data)
            except:
                error_subject = '[{}] PairReversion Failed'.format(datetime.date.today().strftime('%Y-%m-%d'))
                error_message = traceback.format_exc()

                # send notification about error
                notify(error_subject, error_message)

                # print error
                traceback.print_exc()

    def order_callback(self, message):
        """ Wrap Binance message to standard format """
        # print(message)
        if message['e'] == 'ORDER_TRADE_UPDATE':
            if message['o']['x'] == 'TRADE':
                raw_data = message['o']
                order = Order(
                    id=raw_data['i'],
                    custom_id=raw_data['c'],
                    timestamp=raw_data['T'],
                    symbol=raw_data['s'],
                    side=raw_data['S'],
                    quantity=raw_data['q'],
                    executed_quantity=raw_data['z'],
                    price=raw_data['p'],
                    average_price=raw_data['ap'],
                    status=raw_data['X'],
                    type=raw_data['ot'],
                    fee=raw_data['n'] if 'n' in raw_data else 0,
                )
                for callback in self.stream_order_callback:
                    callback(order=order)

        # elif message['e'] == 'ACCOUNT_UPDATE':
        #     account = {'position':
        #         {
        #             position['s']: {
        #                 'price': float(position['ep']),
        #                 'quantity': float(position['pa']),
        #             }
        #             for position in message['a']['P']
        #         }
        #     }
        #     for callback in self.stream_order_callback:
        #         callback(account=account)

    def run(self, strategy):
        """ Run strategy on Binance """

        now = time.time()
        if now % 60 > 55:
            print(f'Approaching end of minute, wait {np.round(65 - now % 60, 2)} secs...')
            time.sleep(65 - now % 60)

        self.symbol = strategy.symbol
        self.stream_kline_callback = [strategy.stream_kline_callback]
        self.stream_order_callback = [strategy.stream_order_callback]

        # aggTrade cache for bar aggregation
        self.aggregation = {
            sym: dict(
                timestamp=0,
                first_id=0,
                last_id=0,
                agg_id=0,
                price=deque(),
                volume=deque(),
            ) for sym in self.symbol
        }

        strategy.set_exchange(self)
        strategy.cache_init()

        while True:
            self.loop.run_until_complete(asyncio.wait([self.auto_refresh(), self.check_aggregation()]))
