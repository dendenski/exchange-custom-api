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
            "-pp",
            "--passphrase",
            type=str,
            default="",
            help="passphrase key",
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

base_url = "https://api.kucoin.com"

def generate_signature(endpoint, api_key, secret_key, passphrase_):
    url = base_url + endpoint
    now = int(time.time() * 1000)
    str_to_sign = str(now) + "GET" + endpoint
    signature = base64.b64encode(hmac.new(secret_key.encode("utf-8"), str_to_sign.encode("utf-8"), hashlib.sha256).digest())
    passphrase = base64.b64encode(hmac.new(secret_key.encode("utf-8"), passphrase_.encode("utf-8"), hashlib.sha256).digest())
    headers = {
        "KC-API-SIGN": signature,
        "KC-API-TIMESTAMP": str(now),
        "KC-API-KEY": api_key,
        "KC-API-PASSPHRASE": passphrase,
        "KC-API-KEY-VERSION": "2",
    }
    response = requests.request("get", url, headers=headers)
    kucoin_json = response.json()
    return kucoin_json

def get_kucoin_trades(pair,start_dt, end_dt, api_key, secret_key, passphrase):
    symbol = pair.split("-")[0]
    start_date_ms = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    end_date_ms = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    trade_list = []
    current_start_ms = start_date_ms
    print("Fetching trades ...")
    while current_start_ms < end_date_ms:
        current_end_ms = current_start_ms + 604800000 
        if(current_end_ms > end_date_ms):
            current_end_ms = end_date_ms
        endpoint =f"/api/v1/fills?startAt={current_start_ms}&endAt={current_end_ms}&pageSize=500&currentPage=1&symbol={pair}"
        kucoin_json = generate_signature(endpoint, api_key, secret_key, passphrase)
        if kucoin_json["code"] != "200000":
            print("error:",kucoin_json)
            print("please wait ...")
            time.sleep(10)
            continue

        current_page = 1
        total_page = kucoin_json["data"]["totalPage"]
        while current_page <= total_page:
            endpoint =f"/api/v1/fills?startAt={current_start_ms}&endAt={current_end_ms}&pageSize=500&currentPage={current_page}"
            kucoin_json = generate_signature(endpoint, api_key, secret_key, passphrase)
            if kucoin_json["code"] != "200000":
                print("error:",kucoin_json)
                print("please wait ...")
                time.sleep(10)
                continue
            total_page = kucoin_json["data"]["totalPage"]
            data = kucoin_json["data"]
            print("currentPage:",kucoin_json["data"]["currentPage"],"/",kucoin_json["data"]["totalPage"])
            if len(data["items"]) != 0:
                trade_list += data["items"]
            current_page += 1
        current_start_ms = current_end_ms 
    df_trades = pd.DataFrame (trade_list)
    cols = ["datetime", "source", "symbol", "type", "amount", "price", "quote_amount", "tradeId", "orderId", "counterOrderId", "liquidity", "forceTaker", "price", "funds", "fee", "feeRate", "feeCurrency", "stop", "tradeType", "createdAt"]
    if len(df_trades) > 0:
        df_trades["createdAt"] = df_trades["createdAt"].apply(pd.to_numeric).apply(pd.to_numeric)
        df_trades["datetime"] = df_trades["createdAt"].apply(lambda x: datetime.fromtimestamp(x/1000))
        df_trades = df_trades.rename(columns={"type": "market_type", "symbol": "pair"})
        df_trades = df_trades.rename(columns={"side": "type", "size": "amount"})
        df_trades[["amount", "price", "fee"]] = df_trades[["amount", "price", "fee"]].apply(pd.to_numeric).apply(pd.to_numeric)
        df_trades["quote_amount"] = df_trades["amount"]*df_trades["price"]
        df_trades["source"] = "kucoin"
        df_trades["symbol"] = symbol
        df_trades = df_trades[cols]
    else:
        df_trades = pd.DataFrame(columns=cols)
    
    return df_trades


def get_kucoin_balance(api_key, secret_key, passphrase):
    endpoint =f"/api/v1/accounts"
    kucoin_json = generate_signature(endpoint, api_key, secret_key, passphrase)
    df_balance = pd.DataFrame (kucoin_json["data"],columns = ["currency", "available", "holds"])
    df_balance = df_balance.rename(columns={"holds": "locked"})
    return df_balance


def get_kucoin_transfer(start_dt, end_dt, api_key, secret_key, passphrase):
    start_date_ms = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    end_date_ms = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S").timestamp()*1000)
    transfer_list = []
    current_start_ms = start_date_ms
    print("Fetching deposit/withdrawal history ...")
    while current_start_ms < end_date_ms:
        current_end_ms = current_start_ms + 86400000 
        if(current_end_ms > end_date_ms):
            current_end_ms = end_date_ms
        endpoint =f"/api/v1/accounts/ledgers?startAt={current_start_ms}&endAt={current_end_ms}&pageSize=500&currentPage=1"
        kucoin_json = generate_signature(endpoint, api_key, secret_key, passphrase)
        if kucoin_json["code"] != "200000":
            print("error:",kucoin_json)
            print("please wait ...")
            time.sleep(10)
            continue
        current_page = 1
        total_page = kucoin_json["data"]["totalPage"]
#         print(current_start_ms, " to ", current_end_ms)
        while current_page <= total_page:
            endpoint =f"/api/v1/accounts/ledgers?startAt={current_start_ms}&endAt={current_end_ms}&pageSize=500&currentPage={current_page}"
            kucoin_json = generate_signature(endpoint, api_key, secret_key, passphrase)
            if kucoin_json["code"] != "200000":
                print("error:",kucoin_json)
                print("please wait ...")
                time.sleep(10)
                continue
            total_page = kucoin_json["data"]["totalPage"]
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

def main():
    cmd_args = CmdlineParser().parse_args()
    key = cmd_args.key
    secret = cmd_args.secret
    passphrase = cmd_args.passphrase
    start_date = cmd_args.start_date
    end_date = cmd_args.end_date
    pair = cmd_args.pair
    method = cmd_args.method
    df = pd.DataFrame()

    if method == "transfers":
        df = get_kucoin_transfer(start_date, end_date, key, secret, passphrase)
        filename = f"kucoin-transfer-history-{pair}-{start_date}-{end_date}.csv"
    elif method == "balance":
        df = get_kucoin_balance(key, secret, passphrase)
        filename = f"kucoin-balance-history-{pair}-{start_date}-{end_date}.csv"
    elif method == "trades":
        df = get_kucoin_trades(pair, start_date, end_date, key, secret, passphrase)
        filename = f"kucoin-trade-history-{pair}-{start_date}-{end_date}.csv"

    print(df)
    if not df.empty:
        df.to_csv(filename,index=False) 

if __name__ == "__main__":
    main()