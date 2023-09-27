import base64
import hashlib
import hmac
import json
import numpy as np
import pandas as pd
import requests
import time
import uuid

from datetime import datetime, timedelta
from exchanges.base_api import BaseApi


class BybitApi(BaseApi):
    base_url = "https://api.bybit.com"
    httpClient=requests.Session()
    recv_window=str(5000)
    

    def HTTP_Request(self, endPoint,method,payload):
        time_stamp=str(int(time.time() * 10 ** 3))
        signature=self.genSignature(payload,time_stamp)
        headers = {
            'X-BAPI-API-KEY': self._api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-TIMESTAMP': time_stamp,
            'X-BAPI-RECV-WINDOW': self.recv_window,
            'Content-Type': 'application/json'
        }
        if(method=="POST"):
            response = self.httpClient.request(method, self.base_url+endPoint, headers=headers, data=payload)
        else:
            response = self.httpClient.request(method, self.base_url+endPoint+"?"+payload, headers=headers)
        return response.json()


    def genSignature(self, payload, time_stamp):
        param_str= str(time_stamp) + self._api_key + self.recv_window + payload
        hash = hmac.new(bytes(self._secret_key, "utf-8"), param_str.encode("utf-8"),hashlib.sha256)
        signature = hash.hexdigest()
        return signature

    def get_trades(self):
        endpoint="/v5/account/transaction-log"
        method="GET"
        trade_list = []
        cursor = ""
        start_time = ""
        end_time = ""
        pair = ""
        if self._start_date != "":
                start_time = '&startTime=' + str(self.start_date_ms) 
        if self._end_date != "":
                end_time = '&endTime=' + str(self.end_date_ms) 
        if self._pair != "":
                pair = "&baseCoin=" + self._pair.split("-")[0]
        while 1:
            params= "limit=50" + cursor + start_time + end_time + pair
            ret = self.HTTP_Request(endpoint,method,params)
            if ret["retCode"] != 0:
                print(ret)
                print("please wait ...")
                time.sleep(10)
                continue
            if ret["result"]["nextPageCursor"]:
                cursor = "&cursor=" + ret["result"]["nextPageCursor"]
                trade_list +=ret["result"]["list"]
            else:
                break
        
        df_trades = pd.DataFrame (trade_list)
        if not df_trades.empty:
            df_trades["transactionTime"] = df_trades["transactionTime"].apply(pd.to_numeric).apply(pd.to_numeric)
            df_trades["datetime"] = df_trades["transactionTime"].apply(lambda x: datetime.fromtimestamp(x/1000))
        
        return df_trades

