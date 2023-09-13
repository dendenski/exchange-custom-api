import hashlib
import hmac
import json
import numpy as np
import pandas as pd
import requests
import time

from datetime import datetime, timedelta
from exchanges.base_api import BaseApi

class GateApi(BaseApi):
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    S_30_DAYS = 2592000

    def gen_sign(self, method, url, query_string=None, payload_string=None):
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode("utf-8"))
        hashed_payload = m.hexdigest()
        s = "%s\n%s\n%s\n%s\n%s" % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(self._secret_key.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
        return {"KEY": self._api_key, "Timestamp": str(t), "SIGN": sign}

    def get_balance(self): 
        url = "/spot/accounts"
        query_param = ""
        sign_headers = self.gen_sign("GET", self.prefix + url, query_param)
        self.headers.update(sign_headers)
        r = requests.request("GET", self.host + self.prefix + url, headers=self.headers)
        json = r.json()
        df_balance = pd.DataFrame (json, columns = ["currency", "available", "locked"])
        return df_balance

    def get_trades(self):
        self._pair = self._pair.replace("-", "_")
        symbol = self._pair.split("_")[0]
        currency_pair = f"currency_pair={self._pair}&" if self._pair != "" else ""
        url = "/spot/my_trades"
        trade_list = []
        curr_start_s = self.start_date_s
        while curr_start_s < self.end_date_s:
            curr_end_s = curr_start_s + self.S_30_DAYS
            if curr_end_s > self.end_date_s:
                curr_end_s = self.end_date_s
            page = 1
            while 1:
                query_param = f"{currency_pair}from={curr_start_s}&to={curr_end_s}&page={page}"
                sign_headers = self.gen_sign("GET", self.prefix + url, query_param)
                self.headers.update(sign_headers)
                r = requests.request("GET", self.host + self.prefix + url + "?" + query_param, headers=self.headers)
                trade_list_temp = r.json()
                if len(trade_list_temp) == 0:
                    break
                else:
                    page += 1
                    trade_list += trade_list_temp
            curr_start_s = curr_end_s
        df_trades = pd.DataFrame (trade_list)

        cols = ["datetime", "currency_pair", "side", "amount", "price", "quote_amount", 
                "id", "create_time", "create_time_ms", "role", "order_id", 
                "fee", "fee_currency", "point_fee", "gt_fee"]
        
        if len(df_trades) > 0:
            df_trades["create_time"] = df_trades["create_time"].apply(pd.to_numeric).apply(pd.to_numeric)
            df_trades["datetime"] = df_trades["create_time"].apply(lambda x: datetime.fromtimestamp(x))
            df_trades[["amount", "price", "fee"]] = df_trades[["amount", "price", "fee"]].apply(pd.to_numeric).apply(pd.to_numeric)
            df_trades["quote_amount"] = df_trades["amount"]*df_trades["price"]
            df_trades = df_trades[cols]
        else:
            df_trades = pd.DataFrame(columns=cols)
        return df_trades

    def get_gate_subaccount_transfer(self):
        url = "/wallet/sub_account_transfers"
        data_list = []
        
        S_30_DAYS = 2592000
        curr_start_s = self.start_date_s
        while curr_start_s < self.end_date_s:
            curr_end_s = curr_start_s + S_30_DAYS
            if curr_end_s > self.end_date_s:
                curr_end_s = self.end_date_s
            offset = 0
            while 1:
                query_param = f"from={curr_start_s}&to={curr_end_s}&offset={offset}&limit=100"
                sign_headers = self.gen_sign("GET", self.prefix + url, query_param)
                self.headers.update(sign_headers)
                r = requests.request("GET", self.host + self.prefix + url + "?" + query_param, headers=self.headers)
                list_temp = r.json()
                if len(list_temp) == 0:
                    break
                else:
                    offset = len(list_temp)
                    data_list += list_temp
            curr_start_s = curr_end_s
            df_transfer = pd.DataFrame(data_list)
            cols = ["datetime",  "transfer", "currency", "amount"]
            def get_direction(direction):
                return "deposit" if direction == "to" else "withdrawal"

            if len(df_transfer) > 0:
                df_transfer["timest"] = df_transfer["timest"].apply(pd.to_numeric).apply(pd.to_numeric)
                df_transfer["datetime"] = df_transfer["timest"].apply(lambda x: datetime.fromtimestamp(x))
                df_transfer["transfer"] = df_transfer["direction"].apply(lambda x: get_direction(x))
                df_transfer = df_transfer[cols]
            else:
                df_transfer = pd.DataFrame(columns=cols)
            df_transfer = df_transfer.sort_values(by=["datetime"])
        return df_transfer

    def get_gate_deposits(self):
        url = "/wallet/deposits"
        data_list = []
        S_30_DAYS = 2592000
        curr_start_s = self.start_date_s
        while curr_start_s < self.end_date_s:
            curr_end_s = curr_start_s + S_30_DAYS
            if curr_end_s > self.end_date_s:
                curr_end_s = self.end_date_s
            offset = 0
            while 1:
                query_param = f"from={curr_start_s}&to={curr_end_s}&offset={offset}&limit=100"
                sign_headers = self.gen_sign("GET", self.prefix + url, query_param)
                self.headers.update(sign_headers)
                r = requests.request("GET", self.host + self.prefix + url + "?" + query_param, headers=self.headers)
                list_temp = r.json()
                if len(list_temp) == 0:
                    break
                else:
                    offset = len(list_temp)
                    data_list += list_temp
            curr_start_s = curr_end_s
            df_transfer = pd.DataFrame(data_list)
            cols = ["datetime",  "transfer", "currency", "amount"]

            if len(df_transfer) > 0:

                df_transfer["timestamp"] = df_transfer["timestamp"].apply(pd.to_numeric).apply(pd.to_numeric)
                df_transfer["datetime"] = df_transfer["timestamp"].apply(lambda x: datetime.fromtimestamp(x))
                df_transfer["transfer"] = "deposit"
                df_transfer = df_transfer[cols]
            else:
                df_transfer = pd.DataFrame(columns=cols)
            df_transfer = df_transfer.sort_values(by=["datetime"])
        return df_transfer

    def get_gate_withdrawals(self):
        url = "/wallet/withdrawals"
        data_list = []
        S_30_DAYS = 2592000
        curr_start_s = self.start_date_s
        while curr_start_s < self.end_date_s:
            curr_end_s = curr_start_s + S_30_DAYS
            if curr_end_s > self.end_date_s:
                curr_end_s = self.end_date_s
            offset = 0
            while 1:
                query_param = f"from={curr_start_s}&to={curr_end_s}&offset={offset}&limit=100"
                sign_headers = self.gen_sign("GET", self.prefix + url, query_param)
                self.headers.update(sign_headers)
                r = requests.request("GET", self.host + self.prefix + url + "?" + query_param, headers=self.headers)
                list_temp = r.json()
                if len(list_temp) == 0:
                    break
                else:
                    offset = len(list_temp)
                    data_list += list_temp
            curr_start_s = curr_end_s
            df_transfer = pd.DataFrame(data_list)
            cols = ["datetime",  "transfer", "currency", "amount"]

            if len(df_transfer) > 0:
                df_transfer["timestamp"] = df_transfer["timestamp"].apply(pd.to_numeric).apply(pd.to_numeric)
                df_transfer["datetime"] = df_transfer["timestamp"].apply(lambda x: datetime.fromtimestamp(x))
                df_transfer["transfer"] = "withdrawal"
                df_transfer = df_transfer[cols]
            else:
                df_transfer = pd.DataFrame(columns=cols)
            df_transfer = df_transfer.sort_values(by=["datetime"])
        return df_transfer

    def get_transfers(self):
        df_deposit = self.get_gate_deposits()
        df_withdrwal = self.get_gate_withdrawals()
        df_subaccount = self.get_gate_subaccount_transfer()
        cols = ["datetime",  "transfer", "currency", "amount"]
        df_transfer = pd.DataFrame(columns=cols)

        df_transfer = pd.concat([df_transfer, df_deposit], ignore_index=True)
        df_transfer = pd.concat([df_transfer, df_withdrwal], ignore_index=True)
        df_transfer = pd.concat([df_transfer, df_subaccount], ignore_index=True)
        df_transfer = df_transfer.sort_values(by=["datetime"])
        return df_transfer