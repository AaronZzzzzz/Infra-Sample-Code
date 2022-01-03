from enum import Enum


class OrderStatus(Enum):
    CANCELED = 'CANCELED'
    NEW = 'NEW'
    FILLED = 'FILLED'
    PARTIALLY_FILLED = 'PARTIALLY_FILLED'


class OrderSide(Enum):
    BUY = 'BUY'
    SELL = 'SELL'
    SHORT = 'SHORT'
    COVER = 'COVER'


class OrderType(Enum):
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'
    STOP = 'STOP'
    STOP_MARKET = 'STOP_MARKET'
    TAKE_PROFIT = 'TAKE_PROFIT'
    TAKE_PROFIT_MARKET = 'TAKE_PROFIT_MARKET'
    TRAILING_STOP_MARKET = 'TRAILING_STOP_MARKET'


class Order:

    def __init__(self, id=None, custom_id=None, timestamp=0, symbol=None, side=None, price=0, average_price=0,
                 quantity=0, notional=0, executed_quantity=0, status=None, type=None, fee=0, error=0, **kwargs):
        self.id = id
        self.custom_id = custom_id
        self.timestamp = float(timestamp)
        self.symbol = symbol
        self.side = OrderSide[side.upper()] if isinstance(side, str) else side
        self.price = float(price)
        self.average_price = float(average_price)
        self.quantity = float(quantity)
        self.notional = float(notional)
        self.executed_quantity = float(executed_quantity)
        self.status = OrderStatus[status.upper()] if isinstance(status, str) else status
        self.type = OrderType[type.upper()] if isinstance(type, str) else type
        self.fee = float(fee)
        self.error = error


class BarData:

    def __init__(self, symbol=None, timestamp=None, open=None, close=None, high=None, low=None, volume=None,
                 turnover=None, count=None, info=None):
        self.symbol = symbol
        self.timestamp = timestamp
        self.open = open
        self.close = close
        self.high = high
        self.low = low
        self.volume = volume
        self.turnover = turnover
        self.count = count
        self.info = info

    def __getitem__(self, item):
        if hasattr(self, item):
            return getattr(self, item)

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)




