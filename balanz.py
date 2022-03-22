import os

import requests
import websocket
import json
from google.cloud import firestore

data = {}

db = firestore.Client()

session = requests.Session()

socket = f'wss://clientes.balanz.com/websocket'

user = os.environ.get('balanzuser')

password = os.environ.get('balanzpassword')

payload = {"data": {"user": user, "pass": password}}

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.74 Safari/537.36 Edg/99.0.1150.46'

login_headers = {'Content-type': 'application/json', 'Accept': 'application/json', 'User-Agent': user_agent,
                 'Referer': 'https://clientes.balanz.com/'}

access_token_doc = db.collection(u'MEPBot').document(u'AccessToken')

access_token = access_token_doc.get().to_dict()['value']

msg = None


def login():
    global access_token, access_token_doc

    r = session.post('https://clientes.balanz.com/api/v1/login', json=payload,
                     headers=login_headers)

    access_token = r.json()['AccessToken']

    if access_token is not None and access_token != '':
        access_token_doc.set(
            {
                u'value': access_token
            }, merge=True)


def on_message(ws, message):
    message = json.loads(message)
    if message['plazo'] == 'CI':
        if message['ticker'] == 'AL30D':
            data['al30d_ask'] = message['pv'] * 100
        if message['ticker'] == 'AL30':
            data['al30_bid'] = message['pc'] * 100
        if message['ticker'] == 'GD30D':
            data['gd30d_ask'] = message['pv'] * 100
        if message['ticker'] == 'GD30':
            data['gd30_bid'] = message['pc'] * 100

    if {'al30d_ask', 'al30_bid', 'gd30d_ask', 'gd30_bid'}.issubset(data.keys()):
        ws.keep_running = False


def on_open(ws):
    ws.send(msg)
    print('Open stream')


def on_error(ws, exception):
    print('Stream error')
    print(exception)


def on_close(ws, status, message):
    login()
    print('Closed stream')
    print('status' + str(status))
    print('message' + str(message))
    if status is None and message is None:
        get_quotes(None)


def get_quotes(request):
    global msg

    msg = json.dumps({"panel": 6, "token": access_token})

    wss_header = {'User-Agent': user_agent, 'Origin': 'https://clientes.balanz.com'}

    wss = websocket.WebSocketApp(socket, on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close,
                                 header=wss_header)

    wss.run_forever()

    return json.dumps(data)


if __name__ == '__main__':
    result = get_quotes(None)
    print(result)
