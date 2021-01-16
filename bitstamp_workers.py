#####################################################################################
# Filename       : bitstamp_workers.py
# Author         : Paul Jamieson
# Created        : 01/12/2021
# Edited         : 01/16/2021
# Python Version : 3.7.7
# Purpose        : Threads to run workers created by web api
#
#
# MIT License
#
# Copyright (c) 2021 Paul Jamieson
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#####################################################################################
import os
import json
import time
from websocket import create_connection
import socket
from bitstamp_sql import BitStampMySql as SQL
import threading

# constants

# Current socket address
URI = "wss://ws.bitstamp.net/"

# Channels
# name                     Event                                            Channel
# Live ticker            - trade                                          - live_trades_[currency_pair]
# Live orders            - order_created, order_changed or order_deleted  - live_orders_[currency_pair]
# Live order book        - data                                           - order_book_[currency_pair]
# Live detail order book - data                                           - detail_order_book_[currency_pair]
# Live full order book   - data                                           - diff_order_book_[currency_pair]

VALID_CHANNELS = ["live_trades", "live_orders", "order_book", "detail_order_book", "diff_order_book"]

# Valid currency pairs
VALID_PAIRS = ["btcusd", "btceur", "btcgbp", "btcpax", "btcusdc", "gbpusd", "gbpeur", "eurusd", "xrpusd", "xrpeur",
               "xrpbtc", "xrpgbp", "xrppax", "ltcusd", "ltceur", "ltcbtc", "ltcgbp", "ethusd", "etheur", "ethbtc",
               "ethgbp", "ethpax", "ethusdc", "bchusd", "bcheur", "bchbtc", "bchgbp", "paxusd", "paxeur", "paxgbp",
               "xlmbtc", "xlmusd", "xlmeur", "xlmgbp", "linkusd", "linkeur", "linkgbp", "linkbtc", "linketh", "omgusd",
               "omgeur", "omggbp", "omgbtc", "usdcusd", "usdceur"]

# Config file

if os.path.exists('config.json'):
    with open('config.json') as f:
        config_file = json.loads(f.read())

    SQLHOST = config_file['sql_host']
    SQLUSER = config_file['sql_user']
    SQLPASSWD = config_file['sql_pass']
    SQLDB = config_file['sql_db']


def start_all_watchers():
    db_conn = SQL(SQLHOST, SQLUSER, SQLPASSWD, SQLDB)
    watchers = db_conn.list_watchers()
    for watcher in watchers:
        WatcherThread(name=watcher[1], channel=watcher[2], currency_pair=watcher[3],
                      output="sql").start()


def get_all_watchers():
    db_conn = SQL(SQLHOST, SQLUSER, SQLPASSWD, SQLDB)
    return db_conn.list_watchers()


def stop_all_watchers():
    for t in threading.enumerate():
        if isinstance(t, WatcherThread):
            t.end(remove=False)
            t.join()


class WatcherThread(threading.Thread):
    def __init__(self, name, channel, currency_pair, output):
        super().__init__(name=name)
        self.running = True
        self.channel = channel
        self.currency_pair = currency_pair
        self.output = output
        self.db_conn = SQL(SQLHOST, SQLUSER, SQLPASSWD, SQLDB)

    def run(self):
        while self.running:
            try:
                self.db_conn.create_watcher(self.getName(), self.channel, self.currency_pair)
                # Open socket with server
                ws = create_connection(URI)
                ws.settimeout(1)

                # Subscribe to a channel
                ws.send(self._make_subscribe_json())

                # Monitor open socket for new data
                self._monitor_subscription(ws)

            except socket.gaierror:
                print(f'Network connection is down, will retry when network connection is re-established.')
                time.sleep(10)

            except Exception as e:
                print(f"ERROR: {type(e)} {e}")
                break

    def end(self, remove=True):
        if remove:
            self.db_conn.delete_watcher(self.getName())
        self.running = False

    def _check_currency_pair(self):
        return False if self.currency_pair not in VALID_PAIRS else True

    def _check_channel(self):
        return False if self.channel not in VALID_CHANNELS else True

    def _make_subscribe_json(self):
        subscription = {
            "event": "bts:subscribe",
            "data": {
                "channel": f"{self.channel}_{self.currency_pair}"
            }
        }
        return json.dumps(subscription)

    def _monitor_subscription(self, open_socket):
        while self.running:
            try:
                resp = json.loads(open_socket.recv())
                if resp:
                    data = resp['data']
                    event = resp['event']
                    channel = resp['channel']
                    if event == 'trade':
                        self._handle_trade(data)
                else:
                    print("empty")
            except (ConnectionAbortedError, TimeoutError):
                open_socket.close()
                print(f'Connection to server has been lost, attempting to reconnect...')
                break
            except Exception as e:
                print(f"ERROR: {type(e)} {e}")
                break

    def _handle_trade(self, trade_data):
        # Sample trade_data
        # {"data": {"buy_order_id": 1 314 580 971 991 040, "amount_str": "0.06400000", "timestamp": "1 609 777 594",
        #           "microtimestamp": "1609777594772000", "id": 139 255 607, "amount": 0.064,
        #           "sell_order_id": 1314580969922560, "price_str": "30859.03", "type": 0, "price": 30859.03},
        #           "event": "trade", "channel": "live_trades_btcusd"}
        sql_trade_data = {
            "id": trade_data["id"], "buy_order_id": trade_data["buy_order_id"],
            "sell_order_id": trade_data["sell_order_id"], "amount": trade_data["amount"],
            "price": trade_data["price"],
            "type": trade_data["type"], "timestamp": trade_data["timestamp"]
        }
        self.db_conn.create_trade(sql_trade_data, self.currency_pair)
