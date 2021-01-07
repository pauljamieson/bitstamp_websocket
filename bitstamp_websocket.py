#####################################################################################
# Filename       : bitstamp_websocket.py
# Author         : Paul Jamieson
# Created        : 01/04/2021
# Edited         : 01/06/2021
# Python Version : 3.7.7
# Purpose        : Open a websocket with the bitstamp public service and receive
#                up to date data on their currency exchange.
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
import sys
import os
import getopt
import json
from websocket import create_connection
from datetime import datetime
import pandas as pd
import socket
from bitstamp_sql import BitStampMySql as SQL

from bitstamp_sql import BitStampMySql

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

# Valid outputs
VALID_OUTPUTS = ["console", "csv", "sql"]

# Config file

with open('config.json') as f:
    config_file = json.loads(f.read())

SQLHOST = config_file['sql_host']
SQLUSER = config_file['sql_user']
SQLPASSWD = config_file['sql_pass']
SQLDB = config_file['sql_db']


def main(channel, currency_pair, output):
    first = True
    # loop to reconnect if connection lost
    while True:
        try:
            # Open socket with server
            ws = create_connection(URI)
            ws.settimeout(1)

            first = True
            # Subscribe to a channel
            ws.send(make_subscribe_json(f"{channel}_{currency_pair}"))

            # Monitor open socket for new data
            print(f'Monitoring socket for new data, press Ctrl-C to exit.')
            monitor_subscription(ws, output)
        except KeyboardInterrupt:
            ws.close()
            print(f"ctrl-c pressed, halting program...")
            quit(0)
        except socket.gaierror:
            if first:
                print(f'Network connection is down, will retry when network connection is re-established.')
                first = False;

        except Exception as e:
            print(f"ERROR: {type(e)} {e}")
            break


def monitor_subscription(open_socket, output):
    while True:
        try:
            resp = json.loads(open_socket.recv())
            if resp:
                data = resp['data']
                event = resp['event']
                channel = resp['channel']
                if event == 'trade':
                    pair = channel.split("_")
                    handle_trade(data, pair[2], output)
            else:
                print("empty")
        except KeyboardInterrupt:
            open_socket.close()
            print(f"ctrl-c pressed, halting program...")
            quit(0)
        except (ConnectionAbortedError, TimeoutError):
            open_socket.close()
            print(f'Connection to server has been lost, attempting to reconnect...')
            break
        except Exception as e:
            print(f"ERROR: {type(e)} {e}")
            break


def handle_trade(trade_data, currency, output):
    # Sample trade_data
    # {"data": {"buy_order_id": 1 314 580 971 991 040, "amount_str": "0.06400000", "timestamp": "1 609 777 594",
    #           "microtimestamp": "1609777594772000", "id": 139 255 607, "amount": 0.064,
    #           "sell_order_id": 1314580969922560, "price_str": "30859.03", "type": 0, "price": 30859.03},
    #           "event": "trade", "channel": "live_trades_btcusd"}

    if output == VALID_OUTPUTS[0]:
        print(f"Trade data")
        print(f'    id: {trade_data["id"]}')
        print(f'    buy_order_id: {trade_data["buy_order_id"]}')
        print(f'    sell_order_id: {trade_data["sell_order_id"]}')
        print(f'    amount: {trade_data["amount"]}')
        print(f'    price: {trade_data["price"]}')
        print(f'    time: {datetime.fromtimestamp(int(trade_data["timestamp"]))}')

    if output == VALID_OUTPUTS[1]:
        data = {"trade_pair": [currency], 'id': [trade_data['id']], 'buy_order_id': [trade_data["buy_order_id"]],
                "sell_order_id": [trade_data["sell_order_id"]], "amount": [trade_data["amount"]],
                "price": [trade_data["price"]], "timestamp": [trade_data["timestamp"]]}

        df = pd.DataFrame(data=data)
        if not os.path.isfile(f'{currency}.csv'):
            df.to_csv(f'{currency}.csv', mode="w", index=False)
        else:
            df.to_csv(f'{currency}.csv', mode="a", index=False, header=False)

    if output == VALID_OUTPUTS[2]:
        sql_trade_data = {
            "id": trade_data["id"], "buy_order_id": trade_data["buy_order_id"],
            "sell_order_id": trade_data["sell_order_id"], "amount": trade_data["amount"], "price": trade_data["price"],
            "type": trade_data["type"], "timestamp": trade_data["timestamp"]
        }
        db.create_trade(sql_trade_data, currency)


def check_currency_pair(pair):
    return False if pair not in VALID_PAIRS else True


def check_channel(channel):
    return False if channel not in VALID_CHANNELS else True


def check_output(output):
    return False if output not in VALID_OUTPUTS else True


def make_subscribe_json(channel):
    output = {
        "event": "bts:subscribe",
        "data": {
            "channel": channel
        }
    }
    return json.dumps(output)


if __name__ == "__main__":

    # Set defaults
    channel = VALID_CHANNELS[0]
    currency_pair = VALID_PAIRS[0]
    output = [VALID_OUTPUTS[0]]

    try:
        opts, args = getopt.getopt(sys.argv[1:], "ho:c:p:", ["channel=", "pair=", "output=", "help"])
    except getopt.GetoptError:
        print(
            f'bitstamp_websocket.py -h -c <channel> -p <currency_pair> --channel=<channel> --pair=<currency_pair> --help')
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print(f'bitstamp_websocket.py -h -c <channel> -p <currency_pair> --channel=<channel> --pair=<currency_pair>'
                  f' --help')
            sys.exit(0)
        elif opt == "--help":
            print(f'bitstamp_websocket.py -h -c <channel> -p <currency_pair> --channel=<channel> --pair=<currency_pair>'
                  f' --help')
            print('')
            print(f'Valid channel names:')
            for channel in VALID_CHANNELS[:-1]:
                print(f'{channel},  ', end="")
            print(f'{VALID_CHANNELS[-1]}')
            print('')
            print(f'Valid currency pairs:')
            for pair in VALID_PAIRS[:-1]:
                print(f'{pair}, ', end="")
            print(f'{VALID_PAIRS[-1]}')
            sys.exit(0)
        elif opt in ("-p", "--pair"):
            if check_currency_pair(arg):
                currency_pair = arg
            else:
                print(f'Currency pair not valid.  Use -h to see all valid pairs.')
                sys.exit(2)
        elif opt in ("-c", "--channel"):
            if check_channel(arg):
                channel = arg
            else:
                print(f'Channel name not valid.  Use -h to see all valid channels.')
                sys.exit(2)
        elif opt in ("-o","--output"):

            if check_output(arg):
                output = arg
                # create db connection of output used
                if arg == VALID_OUTPUTS[2]:
                    db = SQL(SQLHOST, SQLUSER, SQLPASSWD, SQLDB)
                    db.connect()

    main(channel, currency_pair, output)
