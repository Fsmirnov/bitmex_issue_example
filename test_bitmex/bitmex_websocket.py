import csv
import json
import ssl
import sys
import threading
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import List, Dict
from urllib.parse import urlparse, urlunparse

import numpy as np
import websocket

from test_bitmex.logger import logger


# Connects to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without heavily polling the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll as often as it wants.
class BitMEXWebsocket:
    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200

    def __init__(self):
        self.data_path = Path('test_bitmex/data/XBTUSD/order_book_data/')
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.__reset()
        self.last_order_books_update_time = np.datetime64(datetime.now())

    def __del__(self):
        self.exit()

    def connect(self, endpoint="", symbol="XBTUSD", shouldAuth=False):
        '''Connect to the websocket and initialize data stores.'''

        logger.debug("Connecting WebSocket.")
        self.symbol = symbol
        self.shouldAuth = shouldAuth

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        subscriptions = [sub + ':' + symbol for sub in ["orderBookL2"]]
        # subscriptions += ["instrument"]  # We want all of them
        if self.shouldAuth:
            subscriptions += [sub + ':' + symbol for sub in ["order", "execution"]]
            subscriptions += ["margin", "position"]

        # Get WS URL and connect.
        urlParts = list(urlparse(endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe=" + ",".join(subscriptions)
        wsURL = urlunparse(urlParts)
        logger.info(f"Connecting to {wsURL}")
        self.__connect(wsURL)
        logger.info('Connected to WS. Waiting for data images, this may take a moment...')

        # Connected. Wait for partials
        # self.__wait_for_symbol(symbol)
        logger.info(f"{symbol} received. Waiting for account...")
        if self.shouldAuth:
            self.__wait_for_account()
        logger.info('Got all market data. Starting.')

    #
    # Lifecycle methods
    #
    def error(self, err):
        self._error = err
        logger.error(err)
        self.exit()

    def exit(self):
        self.exited = True
        self.ws.close()

    #
    # Private methods
    #

    def __connect(self, wsURL):
        '''Connect to the websocket in a thread.'''
        logger.debug("Starting thread")

        ssl_defaults = ssl.get_default_verify_paths()
        sslopt_ca_certs = {'ca_certs': ssl_defaults.cafile}
        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth()
                                         )

        self.wst = threading.Thread(target=lambda: self.ws.run_forever(sslopt=sslopt_ca_certs))
        self.wst.daemon = True
        self.wst.start()
        logger.info("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while (not self.ws.sock or not self.ws.sock.connected) and conn_timeout and not self._error:
            sleep(1)
            conn_timeout -= 1

        if not conn_timeout or self._error:
            logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            sys.exit(1)

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''

        if self.shouldAuth is False:
            return []

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order'} <= set(self.data):
            sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'quote'} <= set(self.data):
            sleep(0.1)

    def __send_command(self, command, args):
        '''Send a raw command.'''
        self.ws.send(json.dumps({"op": command, "args": args or []}))

    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)

        table = message['table'] if 'table' in message else None
        action = message['action'] if 'action' in message else None
        try:
            if 'subscribe' in message:
                if message['success']:
                    logger.debug("Subscribed to %s." % message['subscribe'])
                else:
                    self.error("Unable to subscribe to %s. Error: \"%s\" Please check and restart." %
                               (message['request']['args'][0], message['error']))
            elif 'status' in message:
                if message['status'] == 400:
                    self.error(message['error'])
                if message['status'] == 401:
                    self.error("API Key incorrect, please check and restart.")
            elif action:

                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                dump_dict: Dict[int, List[List]] = defaultdict(list)
                if action == 'partial':
                    logger.info('Collecting snapshot')
                    for order_data in message['data']:
                        timestamp = int((np.datetime64(order_data['timestamp']) - np.datetime64(
                            '1970-01-01T00:00')) / np.timedelta64(1, 'μs'))
                        dump_dict[timestamp - timestamp % int(timedelta(hours=1) / timedelta(microseconds=1))].append(
                            [order_data['id'],
                             True,
                             True if order_data.get('side') == 'Sell' else False,
                             order_data.get('size'),
                             order_data.get('price'),
                             timestamp]
                        )
                    self.keys[table] = message['keys']
                elif action == 'insert' or action == 'update' or action == 'delete':
                    for order_data in message['data']:
                        timestamp = int((np.datetime64(order_data['timestamp']) - np.datetime64(
                            '1970-01-01T00:00')) / np.timedelta64(1, 'μs'))
                        dump_dict[timestamp - timestamp % int(timedelta(hours=1) / timedelta(microseconds=1))].append(
                            [order_data['id'],
                             False,
                             True if order_data.get('side') == 'Sell' else False,
                             order_data.get('size'),
                             order_data.get('price'),
                             timestamp]
                        )
                else:
                    raise Exception("Unknown action: %s" % action)

                # Every data row pushes to file {timestamp}.csv, where we round timestamps up to hours
                cur_timestamp = int(datetime.now().timestamp() * 1e6)
                for timestamp, save_data in dump_dict.items():
                    new_file: Path = self.data_path.joinpath(f'{timestamp}.csv')
                    with new_file.open(mode='a') as file:
                        csv_writer = csv.writer(file)
                        for data in save_data:
                            csv_writer.writerow(data + [cur_timestamp])
                            if abs(data[-1] - cur_timestamp) > int(timedelta(hours=2) / timedelta(microseconds=1)):
                                logger.debug(f'Late order data: {data}')
        except:
            logger.error(traceback.format_exc())

    def __on_open(self):
        logger.debug("Websocket Opened.")

    def __on_close(self):
        logger.info('Websocket Closed')
        self.exit()

    def __on_error(self, error):
        if not self.exited:
            self.error(error)

    def __reset(self):
        self.data = {}
        self.keys = {}
        self.exited = False
        self._error = None


def findItemByKeys(keys, table, matchData):
    for item in table:
        matched = True
        for key in keys:
            if item[key] != matchData[key]:
                matched = False
        if matched:
            return item


def main_test_bitmex():
    # create console handler and set level to debug
    connected: bool = False
    while True:
        if not connected:
            try:
                ws = BitMEXWebsocket()
                ws.connect("https://ws.bitmex.com/api/v1")
                connected = True
            except Exception as e:
                logger.error(e)
        if not ws.ws.sock.connected:
            connected = False

        if np.datetime64(datetime.now()) - ws.last_order_books_update_time > np.timedelta64(30, 'm'):
            try:
                logger.info('Updating order book')
                ws.ws.send(
                    ws.ws.send(json.dumps({"op": "unsubscribe", "args": ["orderBookL2:XBTUSD"]}))
                )
                sleep(1)
                ws.ws.send(
                    ws.ws.send(json.dumps({"op": "subscribe", "args": ["orderBookL2:XBTUSD"]}))
                )
                ws.last_order_books_update_time = np.datetime64(datetime.now())
            except Exception as e:
                logger.error(e)
                connected = False

        sleep(1)
