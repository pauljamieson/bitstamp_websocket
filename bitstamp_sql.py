#####################################################################################
# Filename       : bitstamp_websocket.py
# Author         : Paul Jamieson
# Created        : 01/06/2021
# Edited         : 01/06/2021
# Python Version : 3.7.7
# Purpose        : Perform CRUD operations for mysql database
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

import mysql.connector


class BitStampMySql:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        self.conn = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )

    def create_trade(self, trade_data, table):
        try:
            with self.conn.cursor() as cursor:
                sql = f'INSERT INTO {table} (id, buy_order_id, sell_order_id, amount, price, type, timestamp) ' \
                      f'VALUES (%s, %s, %s, %s, %s, %s, %s)'
                val = (trade_data["id"], trade_data["buy_order_id"], trade_data["sell_order_id"], trade_data["amount"],
                       trade_data["price"], trade_data["type"], trade_data["timestamp"])
                cursor.execute(sql, val)
                self.conn.commit()
        except Exception as e:
            print(f"Failed to create trade: {e}")

if __name__ == "__main__":
    test = BitStampMySql("192.168.0.150", "root", "Tikatika1", "bitstamp")
    test.connect()
    with test.conn.cursor() as c:
        c.execute("SHOW DATABASES")
        for x in c:
            print(x)

    with test.conn.cursor() as c:
        c.execute("SHOW DATABASES")
        for x in c:
            print(x)

    # ethusd, 1314800356388864, 1314800350928896, 0.51830845, 1017.52, 1609831155
    trade_data = {"id": 222, "buy_order_id": 1314800356388864, "sell_order_id": 1314800350928896,
                  "amount": 0.51830845, "price": 1017.52, "type": 0, "timestamp": 1609831155}
    test.create_trade(trade_data, "ethusd")
