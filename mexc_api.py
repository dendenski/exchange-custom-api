import time
import requests
import json
from requests import post
import hashlib
import hmac
import datetime
import base64
from datetime import datetime, timedelta
import pytz
import pandas as pd
import numpy as np
import argparse
from urllib.parse import urlencode, quote
import urllib.parse

class CmdlineParser(argparse.ArgumentParser):
    def __init__(self):
        super().__init__(description="Get Trade History")
        self.add_argument(
            "-k",
            "--key",
            type=str,
            default="",
            help="API key",
        )
        self.add_argument(
            "-z",
            "--secret",
            type=str,
            default="",
            help="SECRET key",
        )
        self.add_argument(
            "-s",
            "--start_date",
            type=str,
            default="",
            help="start date  format YYYY-MM-DD HH:MM:SS",
        )
        self.add_argument(
            "-e", 
            "--end_date", 
            type=str, 
            default="", 
            help="end date format YYYY-MM-DD HH:MM:SS"
        )
        self.add_argument(
            "-p", 
            "--pair", 
            type=str, 
            default="", 
            help="Market pair format XRD-USDT"
        )
        self.add_argument(
            "-m", 
            "--method", 
            type=str, 
            default="", 
            help="Select method, ex. trades, transfers, balance"
        )

api = "/api/v3"
hosts_v3 = "https://api.mexc.com"

def _get_server_time():
        return requests.request("get", "https://api.mexc.com/api/v3/time").json()["serverTime"]

def _sign_v3(mexc_secret, req_time, sign_params=None):
    if sign_params:
        sign_params = urlencode(sign_params, quote_via=quote)
        to_sign = "{}&timestamp={}".format(sign_params, req_time)
    else:
        to_sign = "timestamp={}".format(req_time)
    sign = hmac.new(mexc_secret.encode("utf-8"), to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    return sign

def get_server_time_v2():
    return int(time.time()*1000)

def sign_v2(mexc_key, mexc_secret, sign_params=None):
    params = (mexc_key, get_server_time_v2())
    if sign_params:
        params = "%s%s%s" % (mexc_key, get_server_time_v2(), sign_params)
    else:
        params = "%s%s" % (mexc_key, get_server_time_v2())
    params = hmac.new(mexc_secret.encode(), params.encode(),
                      hashlib.sha256).hexdigest()
    return params

def sign_request(mexc_key, mexc_secret, method, url, params=None):
    url = "{}{}".format(hosts_v3, url)
    req_time = _get_server_time()
    if params:
        params["signature"] = _sign_v3(mexc_secret=mexc_secret, req_time=req_time, sign_params=params)
    else:
        params = {}
        params["signature"] = _sign_v3(mexc_secret=mexc_secret, req_time=req_time)
    params["timestamp"] = req_time
    headers = {
        "x-mexc-apikey": mexc_key,
        "Content-Type": "application/json",
    }
    return requests.request(method, url, params=params, headers=headers)

def get_mexc_balance(mexc_key, mexc_secret):
    method = "GET"
    url = "{}{}".format(api, "/account")
    response = sign_request(mexc_key, mexc_secret, method, url)
    df = pd.DataFrame (response.json()["balances"])
    return df

def get_mexc_transfer_data(path ,startTime, endTime, mexc_key, mexc_secret):
    start_date_ms = int(datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    end_date_ms = int(datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    host = "https://www.mexc.com"
    method = 'GET'
    BASE_URL = host
    url_base = '{}{}'.format(BASE_URL, path)
    data_list = []
    MS_10_DAYS = 864000000
    curr_start_ms = start_date_ms
    while curr_start_ms < end_date_ms:
        curr_end_ms = curr_start_ms + MS_10_DAYS
        if curr_end_ms > end_date_ms:
            curr_end_ms = end_date_ms
        page = 1
        while 1:
            data_orignal = {}
            data_orignal.update({"page_size": 50})
            data_orignal.update({"page_unm": page})
            data_orignal.update({"start_time": curr_start_ms})
            data_orignal.update({"end_time": curr_end_ms})
            
            data = '&'.join('{}={}'.format(
                    i, data_orignal[i]) for i in sorted(data_orignal))
            params = sign_v2(mexc_key, mexc_secret, sign_params=data)
            headers = {
                "ApiKey": mexc_key,
                "Request-Time": str(get_server_time_v2()),
                "Signature": params,
                "Content-Type": "application/json"
            }
            url = "%s%s%s" % (url_base, "?", data)
            response = requests.request(method, url, headers=headers)
            res = response.json()
            if res["code"] != 200:
                print(res["code"])
                continue
            data_resp = res["data"]
            data_list += data_resp["result_list"]
            if data_resp["total_page"] <= page:
                break
            else:
                page +=1
        curr_start_ms = curr_end_ms
        df_transfer = pd.DataFrame(data_list)
    return df_transfer


def get_mexc_transfer(startTime, endTime, mexc_key, mexc_secret):
    deposit_path = '/open/api/v2/asset/deposit/list'
    df_deposit = get_mexc_transfer_data(deposit_path, startTime, endTime, mexc_key, mexc_secret)
    df_deposit["transfer"] = "deposit"
    withdraw_path = "/open/api/v2/asset/withdraw/list"
    df_withdraw = get_mexc_transfer_data(withdraw_path, startTime, endTime, mexc_key, mexc_secret)
    df_withdraw["transfer"] = "withdrawal"
    cols = ["create_time",  "transfer", "currency", "amount"]
    df_transfer = pd.DataFrame(columns=cols)

    df_transfer = pd.concat([df_transfer, df_deposit], ignore_index=True)
    df_transfer = pd.concat([df_transfer, df_withdraw], ignore_index=True)
    df_transfer = df_transfer[cols]
    df_transfer = df_transfer.sort_values(by=["create_time"])
    return df_transfer

def get_mexc_transfers_subaccount(mexc_key, mexc_secret, startTime, endTime):
    start_date_ms = int(datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    end_date_ms = int(datetime.strptime(endTime, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    last_ms = end_date_ms
    data_list = []
    page = 1
    while 1:
        params = {
            "fromAccountType": "SPOT",
            "toAccountType" : "SPOT",
            "startTime" : start_date_ms,
            "endTime" : last_ms,
            "page" : page
        }
        method = "GET"
        url = "{}{}".format(api, "/capital/sub-account/universalTransfer")
        response = sign_request(mexc_key, mexc_secret, method, url, params=params)
        print(response.json())
        res = response.json()
        if len(res["result"]) == 0:
            break
        list_temp = res["result"]
        data_list += list_temp
        page += 1
        
    df_transfers = pd.DataFrame(data_list)
    return df_transfers

def main():
    cmd_args = CmdlineParser().parse_args()
    key = cmd_args.key
    secret = cmd_args.secret
    start_date = cmd_args.start_date
    end_date = cmd_args.end_date
    pair = cmd_args.pair
    method = cmd_args.method
    df = pd.DataFrame()

    if method == "transfers":
        df = get_mexc_transfer(start_date, end_date, key, secret)
        filename = f"mexc-transfer-history-{pair}-{start_date}-{end_date}.csv"
    elif method == "balance":
        df = get_mexc_balance(key, secret)
        filename = f"mexc-balance-history-{pair}-{start_date}-{end_date}.csv"
    elif method == "transfers_sub":
        df = get_mexc_transfers_subaccount(key, secret, start_date, end_date,)
        filename = f"mexc-transfer-history-{pair}-{start_date}-{end_date}.csv"

    print(df)
    if not df.empty:
        df.to_csv(filename,index=False) 

if __name__ == "__main__":
    main()