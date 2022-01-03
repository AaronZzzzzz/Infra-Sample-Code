import asyncio
import time
import json
import threading
from exchange.enums import Order, OrderType, OrderSide

ZERO_LEVEL = 1e-8


class PairOrderExecution(threading.Thread):

    """
    Pair Orders Execution Workflow:

    1. Send LIMIT orders to exchange
    2. Wait {$wait} seconds for order fulfillment
    3. Cancel orders and send remaining notional as MARKET

    """

    def __init__(self, logger=None):
        super(PairOrderExecution, self).__init__()
        # self.loop = asyncio.new_event_loop()
        self.logger = logger

    def logging(self, log_dict):
        """ Status logging function """
        if self.logger:
            self.logger.log('TRADING', json.dumps(log_dict))
        else:
            print(log_dict)

    def set_exchange(self, exchange):
        """ Set execution channel """
        self.exchange = exchange
        if hasattr(exchange, 'loop'):
            self.loop = self.exchange.loop
        else:
            self.loop = asyncio.new_event_loop()
    #
    # def run(self):
    #     """ Start async event loop """
    #     self.loop.run_forever()

    async def force_execute(self, wait):
        """ Force execution after wait """

        # sleep
        r = await asyncio.sleep(wait)

        # cancel all existing orders and replace as market
        market_orders = list()
        for symbol, order in self.original_orders.items():
            _ = self.exchange.cancel(symbol=symbol, custom_id=order.custom_id)
            executed_quantity = sum(o['quantity'] for o in self.execution_status[symbol].values())
            remaining_notional = order.price * abs(order.quantity - executed_quantity)
            if remaining_notional >= 5.1:
                print(f'{symbol} remaining ${remaining_notional}')
                market_orders.append(Order(
                    symbol=symbol,
                    quantity=order.quantity - executed_quantity,
                    side=order.side,
                    type=OrderType.MARKET,
                    custom_id=None,
                ))

        if len(market_orders) > 0:
            self.exchange.place_batch(market_orders)
            self.logging({
                'event': 'pair_order_execution_force_execution',
                'data': {o.symbol: o.quantity for o in market_orders}
            })

        # sleep
        r = await asyncio.sleep(5)

        # log execution PnL
        execution_pnl = dict()
        execution_sum = {
            'notional': 0,
            'markout_pnl': 0,
            'fee_pnl': 0,
            'total': 0,
        }
        for symbol, order in self.original_orders.items():
            original_side = 1 if order.side == OrderSide.BUY else -1
            executed_notional = sum(o['notional'] for o in self.execution_status[symbol].values())
            executed_fee = sum(o['fee'] for o in self.execution_status[symbol].values())
            markout_pnl = (order.notional - executed_notional) * original_side
            fee_pnl = executed_fee * -1
            execution_pnl[symbol] = {
                'notional': order.notional,
                'markout_pnl': markout_pnl,
                'fee_pnl': fee_pnl,
                'total': markout_pnl + fee_pnl,
            }
            execution_sum['notional'] += order.notional
            execution_sum['markout_pnl'] += markout_pnl
            execution_sum['fee_pnl'] += fee_pnl
            execution_sum['total'] += markout_pnl + fee_pnl
        execution_pnl['aggregate'] = execution_sum
        self.logging({
            'event': 'pair_order_execution_pnl',
            'data': execution_pnl
        })

    def stream_order_callback(self, order=None, account=None):
        """ Callback function for order status """
        if order is not None:
            if order.symbol in self.execution_status:
                # update executed notional and quantity
                self.execution_status[order.symbol][order.id] = {
                    'quantity': order.executed_quantity,
                    'notional': order.average_price * order.executed_quantity,
                    'fee': self.execution_status[order.symbol].get(order.id, {}).get('fee', 0) + order.fee
                }
                self.logging({
                    'event': 'pair_order_execution_filling',
                    'data': self.execution_status
                })

    def pair_order(self, orders, wait=10):
        """ Main function to place pair orders """
        # remove invalid orders
        orders = [o for o in orders if (o.price * o.quantity) > 5.1]

        # logging
        self.original_orders = dict()
        for i, order in enumerate(orders):
            order.custom_id = hex(int(time.time() * 10000 + i))
            self.logging({
                'event': 'pair_order_execution_initial_placement',
                'data': {
                    'custom_id': str(order.custom_id),
                    'symbol': str(order.symbol),
                    'side': str(order.side.value),
                    'quantity': str(order.quantity),
                    'price': str(order.price),
                    'type': str(order.type.value),
                }
            })
            self.original_orders[order.symbol] = order
        self.execution_status = {order.symbol: dict() for order in orders}
        recv_orders = self.exchange.place_batch(orders)
        self.loop.call_soon_threadsafe(asyncio.create_task, self.force_execute(wait))




