import json
import os

from pyhomebroker import HomeBroker
from pyhomebroker.common import SessionException


def on_open(online):
    print('=================== CONNECTION OPENED ====================')


def on_personal_portfolio(online, portfolio_quotes, order_book_quotes):
    print('------------------- Personal Portfolio -------------------')
    print(portfolio_quotes)
    print('------------ Personal Portfolio - Order Book -------------')
    print(order_book_quotes)


def on_order_book(online, quotes):
    global data
    print('------------------ Order Book (Level 2) ------------------')
    ask = quotes.ask.reset_index().iloc[0]
    bid = quotes.bid.reset_index().iloc[0]
    if ask['symbol'] == 'AL30D':
        data['al30d_ask'] = ask['ask']
    if bid['symbol'] == 'AL30':
        data['al30_bid'] = bid['bid']
    if ask['symbol'] == 'GD30D':
        data['gd30d_ask'] = ask['ask']
    if bid['symbol'] == 'GD30':
        data['gd30_bid'] = bid['bid']


def on_error(online, exception, connection_lost):
    print('@@@@@@@@@@@@@@@@@@@@@@@@@ Error @@@@@@@@@@@@@@@@@@@@@@@@@@')
    print(exception)


def on_close(online):
    print('=================== CONNECTION CLOSED ====================')


data = {}

dni = os.environ.get('dni')
user = os.environ.get('user')
password = os.environ.get('password')
broker_id = int(os.environ.get('broker_id'))

hb = HomeBroker(broker_id,
                on_open=on_open,
                on_personal_portfolio=on_personal_portfolio,
                on_order_book=on_order_book,
                on_error=on_error,
                on_close=on_close)

hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)
print('Succesfully logged')


def init_process(request):
    try:
        hb.online.connect()

        get_al30_quote()
        get_al30d_quote()
        get_gd30_quote()
        get_gd30d_quote()

    except SessionException as ex:
        hb.auth.login(dni=dni, user=user, password=password, raise_exception=True)

        hb.online.connect()

        hb.online.subscribe_personal_portfolio()

        get_al30_quote()
        get_al30d_quote()
        get_gd30_quote()
        get_gd30d_quote()

    return json.dumps(data)


def get_al30_quote():
    hb.online.connect()

    hb.online.subscribe_order_book('AL30', 'spot')

    hb.online.disconnect()


def get_al30d_quote():
    hb.online.connect()

    hb.online.subscribe_order_book('AL30D', 'spot')

    hb.online.disconnect()


def get_gd30_quote():
    hb.online.connect()

    hb.online.subscribe_order_book('GD30', 'spot')

    hb.online.disconnect()


def get_gd30d_quote():
    hb.online.connect()

    hb.online.subscribe_order_book('GD30D', 'spot')

    hb.online.disconnect()


if __name__ == '__main__':
    result = init_process(None)
    print(result)
