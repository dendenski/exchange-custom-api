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


class BitmartApi(BaseApi):
    base_url = "https://api-cloud.bitmart.com"
    def generate_keyed(self, endpoint):
        url = self.base_url + endpoint
        headers = {
            'Content-Type': 'application/json',
            'X-BM-KEY': self._api_key,
            'X-BM-SIGNATURE': self._secret_key,
        }
        response = requests.get(url, headers=headers)
        json = response.json()
        return json

    def get_timestamp():
        return str(datetime.datetime.now().timestamp() * 1000).split('.')[0]

    def sign(message, secret_key):
        mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        return mac.hexdigest()

    def pre_substring(timestamp, memo, body):
        return f'{str(timestamp)}#{memo}#{body}'


    def get_balance(self):
        endpoint =f"/account/v1/wallet"
        ret_json = self.generate_keyed(endpoint)
        df_balance = pd.DataFrame (ret_json["data"]["wallet"])
        return df_balance

    def get_deposit_withdraw(self, transfer_type):
        endpoint = f"/account/v2/deposit-withdraw/history"
        params = f"?N=100&operation_type={transfer_type}"
        ret_json = self.generate_keyed(endpoint+params)
        return ret_json["data"]["records"]

    def get_transfers(self):
        transfer_list = []
        transfer_list += self.get_deposit_withdraw("deposit")
        transfer_list += self.get_deposit_withdraw("withdraw")
        df_transfers = pd.DataFrame (transfer_list)
        return df_transfers
