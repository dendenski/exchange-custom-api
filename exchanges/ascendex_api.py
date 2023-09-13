import time
import hashlib
import hmac
import requests
import json
import pandas as pd
import numpy as np
import base64
from datetime import datetime, timedelta
from exchanges.base_api import BaseApi

class AscendexApi(BaseApi):
    account = "cash"
    host = "https://ascendex.com"
    ROUTE_PREFIX = "api/pro/v1"
    ROUTE_PREFIX_V2 = "api/pro/data/v2"

    def utc_timestamp(self):
        return int(round(time.time() * 1e3))

    def sign(self, msg):
        msg = bytearray(msg.encode("utf-8"))
        hmac_key = base64.b64decode(self._secret_key)
        signature = hmac.new(hmac_key, msg, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode("utf-8")
        return signature_b64

    def make_auth_headers(self, path):
        timestamp = self.utc_timestamp()
        if isinstance(timestamp, bytes):
            timestamp = timestamp.decode("utf-8")
        elif isinstance(timestamp, int):
            timestamp = str(timestamp)
        msg = f"{timestamp}+{path}"
        header = {
            "x-auth-key": self._api_key,
            "x-auth-signature": self.sign(msg),
            "x-auth-timestamp": timestamp,
        }
        return header

    def parse_response(self,res):
        if res is None:
            return None 
        elif res.status_code == 200:
            obj = json.loads(res.text)
            return obj
        else:
            print(f"request failed, error code = {res.status_code}")
        print(res.text)

    def get_balance(self):
        headers = self.make_auth_headers("balance")
        url = f"{self.host}/{self._group}/{self.ROUTE_PREFIX}/{self.account}/balance"
        params = dict(asset=None, showAll=True)
        res = requests.request('GET', url, headers=headers, params=params)
        asd_json = self.parse_response(res)
        df_balance = pd.DataFrame (asd_json["data"])
        return df_balance

    def get_trades(self):
        pair = self._pair.replace("-", "/") if self._pair != "" else None
        url = f"{self.host}/{self.ROUTE_PREFIX_V2}/order/hist"
        df_trades = pd.DataFrame()
        seqNum = None
        while 1:
            headers = self.make_auth_headers("data/v2/order/hist")
            params = dict(account="cash",
                        limit=1000,
                        startTime=self.start_date_ms,
                        endTime=self.end_date_ms,
                        seqNum = seqNum,
                        symbol=pair
                        )
            res = requests.request('GET', url, headers=headers, params=params)
            asd_json = self.parse_response(res)
            df_trades_temp = pd.DataFrame (asd_json["data"])
            if not df_trades_temp.empty:
                seqNum = df_trades_temp.iloc[-1]["seqNum"] + 1
                df_trades_temp["lastExecTime"] = df_trades_temp["lastExecTime"].apply(pd.to_numeric).apply(pd.to_numeric)
                df_trades_temp["datetime"] = df_trades_temp["lastExecTime"].apply(lambda x: datetime.fromtimestamp(x/1000))
                df_trades_temp = df_trades_temp[df_trades_temp["fillQty"] != "0"]
                cols = ["datetime", "symbol", "side", "orderQty", "price", "status", "fillQty", "fee", "feeAsset", "seqNum", "lastExecTime"]
                df_trades_temp = df_trades_temp[cols]
                df_trades = pd.concat([df_trades, df_trades_temp], ignore_index=True)
            else:
                break
            

        if not df_trades.empty:
            cols = ["datetime", "symbol", "side", "orderQty", "price", "status", "fillQty", "fee", "feeAsset", "seqNum", "lastExecTime"]
            df_trades = df_trades[cols]
        return df_trades

    def get_transfers(self):
        url = f"{self.host}/{self.ROUTE_PREFIX}/wallet/transactions"
        df_transfers = pd.DataFrame()
        headers = self.make_auth_headers("wallet/transactions")
        page=1
        while 1:
            params = dict(account="cash",
                        page=page,
                        pageSize=50
                        )
            res = requests.request('GET', url, headers=headers, params=params)
            asd_json = self.parse_response(res)
            print(asd_json["data"]["data"])
            df_transfers_temp = pd.DataFrame (asd_json["data"]["data"])
            df_transfers_temp["time"] = df_transfers_temp["time"].apply(pd.to_numeric).apply(pd.to_numeric)
            df_transfers_temp["datetime"] = df_transfers_temp["time"].apply(lambda x: datetime.fromtimestamp(x/1000))
            df_transfers = pd.concat([df_transfers, df_transfers_temp], ignore_index=True)
            if not asd_json["data"]["hasNext"]:
                break
            else:
                page += 1
        cols = ["datetime", "status", "transactionType", "asset", "amount", "commission"]
        df_transfers = df_transfers[cols]
        df_transfers = df_transfers.sort_values(by=["datetime"])
        return df_transfers


