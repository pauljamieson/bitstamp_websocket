#####################################################################################
# Filename       : bitstamp_sql.py
# Author         : Paul Jamieson
# Created        : 01/06/2021
# Edited         : 01/13/2021
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
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

    def create_watcher(self, name, channel, currency_pair):
        try:
            with self.conn.cursor() as cursor:
                sql = f'INSERT IGNORE INTO watchers (name, channel, currency_pair) VALUES (%s, %s, %s)'
                values = (name, channel, currency_pair)
                cursor.execute(sql, values)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f'Failed to create watcher entry({e}).')

    def delete_watcher(self, name):
        try:
            with self.conn.cursor() as cursor:
                sql = f'DELETE FROM watchers WHERE name = %s'
                values = (name,)
                cursor.execute(sql, values)
                self.conn.commit()
        except Exception as e:
            print(f'Failed to delete watcher entry({e}).')

    def list_watchers(self):
        try:
            with self.conn.cursor() as cursor:
                sql = f'SELECT * FROM watchers'
                cursor.execute(sql)
                results = cursor.fetchall()
                return results
        except Exception as e:
            print(f'Failed to delete watcher entry({e}).')
            return "Failed to list watchers."

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

