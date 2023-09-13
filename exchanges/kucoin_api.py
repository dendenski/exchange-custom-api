import base64
import hashlib
import hmac
import json
import numpy as np
import pandas as pd
import requests
import time

from datetime import datetime, timedelta
from exchanges.base_api import BaseApi


class KucoinApi(BaseApi):
    base_url = "https://api.kucoin.com"
    def generate_signature(self, endpoint):
        url = self.base_url + endpoint
        now = int(time.time() * 1000)
        str_to_sign = str(now) + "GET" + endpoint
        signature = base64.b64encode(hmac.new(self._secret_key.encode("utf-8"), str_to_sign.encode("utf-8"), hashlib.sha256).digest())
        passphrase = base64.b64encode(hmac.new(self._secret_key.encode("utf-8"), self._passphrase_key.encode("utf-8"), hashlib.sha256).digest())
        headers = {
            "KC-API-SIGN": signature,
            "KC-API-TIMESTAMP": str(now),
            "KC-API-KEY": self._api_key,
            "KC-API-PASSPHRASE": passphrase,
            "KC-API-KEY-VERSION": "2",
        }
        response = requests.request("get", url, headers=headers)
        kucoin_json = response.json()
        return kucoin_json

    def get_balance(self):
        endpoint =f"/api/v1/accounts"
        kucoin_json = self.generate_signature(endpoint)
        df_balance = pd.DataFrame (kucoin_json["data"],columns = ["currency", "available", "holds"])
        df_balance = df_balance.rename(columns={"holds": "locked"})
        return df_balance

    def get_trades(self):
        trade_list = []
        current_start_ms = self.start_date_ms
        currency_pair = f"&symbol={self._pair}" if self._pair != "" else ""
        print("Fetching trades ...")
        while current_start_ms < self.end_date_ms:
            current_end_ms = current_start_ms + 604800000 
            if(current_end_ms > self.end_date_ms):
                current_end_ms = self.end_date_ms
            current_page = 1
            total_page = 1
            while current_page <= total_page:
                endpoint =f"/api/v1/fills?startAt={current_start_ms}&endAt={current_end_ms}&pageSize=500&currentPage={current_page}{currency_pair}"
                kucoin_json = self.generate_signature(endpoint)
                if kucoin_json["code"] != "200000":
                    print("error:",kucoin_json)
                    print("please wait ...")
                    time.sleep(10)
                    continue
                total_page = kucoin_json["data"]["totalPage"]
                if total_page == 0:
                    break
                data = kucoin_json["data"]
                print("currentPage:",kucoin_json["data"]["currentPage"],"/",kucoin_json["data"]["totalPage"])
                if len(data["items"]) != 0:
                    trade_list += data["items"]
                current_page += 1
            current_start_ms = current_end_ms 
        df_trades = pd.DataFrame (trade_list)
        cols = ["datetime", "symbol", "type", "amount", "price", "quote_amount", "tradeId", "orderId", "counterOrderId", "liquidity", "forceTaker", "price", "funds", "fee", "feeRate", "feeCurrency", "stop", "tradeType", "createdAt"]
        if len(df_trades) > 0:
            df_trades["createdAt"] = df_trades["createdAt"].apply(pd.to_numeric).apply(pd.to_numeric)
            df_trades["datetime"] = df_trades["createdAt"].apply(lambda x: datetime.fromtimestamp(x/1000))
            df_trades = df_trades.rename(columns={"side": "type", "size": "amount"})
            df_trades[["amount", "price", "fee"]] = df_trades[["amount", "price", "fee"]].apply(pd.to_numeric).apply(pd.to_numeric)
            df_trades["quote_amount"] = df_trades["amount"]*df_trades["price"]
            df_trades = df_trades[cols]
        else:
            df_trades = pd.DataFrame(columns=cols)
        return df_trades

    def get_transfers(self):
        transfer_list = []
        current_start_ms = self.start_date_ms
        print("Fetching deposit/withdrawal history ...")
        while current_start_ms < self.end_date_ms:
            current_end_ms = current_start_ms + 86400000 
            if(current_end_ms > self.end_date_ms):
                current_end_ms = self.end_date_ms
            current_page = 1
            total_page = 1
            while current_page <= total_page:
                endpoint =f"/api/v1/accounts/ledgers?startAt={current_start_ms}&endAt={current_end_ms}&pageSize=500&currentPage={current_page}"
                kucoin_json = self.generate_signature(endpoint)
                if kucoin_json["code"] != "200000":
                    print("error:",kucoin_json)
                    print("please wait ...")
                    time.sleep(10)
                    continue
                total_page = kucoin_json["data"]["totalPage"]
                if total_page == 0:
                    break
                data = kucoin_json["data"]
                print("currentPage:",kucoin_json["data"]["currentPage"],"/",kucoin_json["data"]["totalPage"])
                if len(data["items"]) != 0:
                    transfer_list += data["items"]
                current_page += 1
            current_start_ms = current_end_ms
        df_transfers = pd.DataFrame (transfer_list)
        def get_direction(direction):
                return "deposit" if direction == "in" else "withdrawal"
        
        if len(df_transfers) > 0:
            bizType_list = ["Sub-account transfer", "Deposit", "Withdrawal"]
            df_transfers = df_transfers[df_transfers["bizType"].isin(bizType_list)]
            df_transfers["createdAt"] = df_transfers["createdAt"].apply(pd.to_numeric).apply(pd.to_numeric)
            df_transfers["datetime"] = df_transfers["createdAt"].apply(lambda x: datetime.fromtimestamp(x/1000))
            cols = ["datetime", "bizType", "transfer", "currency", "amount", "fee", "balance", "id",  "accountType", "direction", "createdAt", "context"]
            df_transfers["transfer"] = df_transfers["direction"].apply(lambda x: get_direction(x))
            df_transfers = df_transfers[cols]
            df_transfers = df_transfers.sort_values(by=["datetime"])
        else:
            df_transfers = df_transfers[cols]
        return df_transfers
