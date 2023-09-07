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
import argparse
import pandas as pd
import numpy as np

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
            help="Market pair format XRD_USDT"
        )
        self.add_argument(
            "-m", 
            "--method", 
            type=str, 
            default="", 
            help="Select method, ex. trades, transfers, balance"
        )

# Gate_io api functions
def gen_sign(gate_key, gate_secret, method, url, query_string=None, payload_string=None):
        t = time.time()
        m = hashlib.sha512()
        m.update((payload_string or "").encode("utf-8"))
        hashed_payload = m.hexdigest()
        s = "%s\n%s\n%s\n%s\n%s" % (method, url, query_string or "", hashed_payload, t)
        sign = hmac.new(gate_secret.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
        return {"KEY": gate_key, "Timestamp": str(t), "SIGN": sign}

def get_gate_balance(gate_key, gate_secret):
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    url = "/spot/accounts"
    query_param = ""
    sign_headers = gen_sign(gate_key, gate_secret, "GET", prefix + url, query_param)
    headers.update(sign_headers)
    r = requests.request("GET", host + prefix + url, headers=headers)
    json = r.json()
    df_balance = pd.DataFrame (json, columns = ["currency", "available", "locked"])
    return df_balance

def get_gate_trades(pair, start_df, end_dt, gate_key, gate_secret):
    symbol = pair.split("_")[0]
    start_date_ms = int(datetime.strptime(start_df, "%Y-%m-%d %H:%M:%S").timestamp())
    end_date_ms = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}
    currency_pair = f"currency_pair={pair}&" if pair != "" else ""
    url = "/spot/my_trades"
    trade_list = []
    
    S_30_DAYS = 2592000
    curr_start_ms = start_date_ms
    while curr_start_ms < end_date_ms:
        curr_end_ms = curr_start_ms + S_30_DAYS
        if curr_end_ms > end_date_ms:
            curr_end_ms = end_date_ms
        page = 1
        while 1:
            query_param = f"{currency_pair}from={curr_start_ms}&to={curr_end_ms}&page={page}"
            sign_headers = gen_sign(gate_key, gate_secret, "GET", prefix + url, query_param)
            headers.update(sign_headers)
            r = requests.request("GET", host + prefix + url + "?" + query_param, headers=headers)
            trade_list_temp = r.json()
            if len(trade_list_temp) == 0:
                break
            else:
                page += 1
                trade_list += trade_list_temp
        curr_start_ms = curr_end_ms
    df_trades = pd.DataFrame (trade_list)

    cols = ["datetime", "source", "symbol", "type", "amount", "price", "quote_amount", "id", "create_time", "create_time_ms", "currency_pair", "role", "order_id", "fee", "fee_currency", "point_fee", "gt_fee"]
    
    if len(df_trades) > 0:
        df_trades["create_time"] = df_trades["create_time"].apply(pd.to_numeric).apply(pd.to_numeric)
        df_trades["datetime"] = df_trades["create_time"].apply(lambda x: datetime.fromtimestamp(x))
        df_trades["symbol"] = symbol
        df_trades = df_trades.rename(columns={"side": "type"})
        df_trades[["amount", "price", "fee"]] = df_trades[["amount", "price", "fee"]].apply(pd.to_numeric).apply(pd.to_numeric)
        df_trades["quote_amount"] = df_trades["amount"]*df_trades["price"]
        df_trades["source"] = "gate"
        df_trades = df_trades[cols]
    else:
        df_trades = pd.DataFrame(columns=cols)
    
    return df_trades

def get_gate_subaccount_transfer(start_dt, end_dt, gate_key, gate_secret):
    start_date_ms = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    end_date_ms = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    url = "/wallet/sub_account_transfers"
    data_list = []
    
    S_30_DAYS = 2592000
    curr_start_ms = start_date_ms
    while curr_start_ms < end_date_ms:
        curr_end_ms = curr_start_ms + S_30_DAYS
        if curr_end_ms > end_date_ms:
            curr_end_ms = end_date_ms
        offset = 0
        while 1:
            query_param = f"from={curr_start_ms}&to={curr_end_ms}&offset={offset}&limit=100"
            sign_headers = gen_sign(gate_key, gate_secret, "GET", prefix + url, query_param)
            headers.update(sign_headers)
            r = requests.request("GET", host + prefix + url + "?" + query_param, headers=headers)
            list_temp = r.json()
            if len(list_temp) == 0:
                break
            else:
                offset = len(list_temp)
                data_list += list_temp
        curr_start_ms = curr_end_ms
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


def get_gate_deposits(start_dt, end_dt, gate_key, gate_secret):
    start_date_ms = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    end_date_ms = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    url = "/wallet/deposits"
    data_list = []
    S_30_DAYS = 2592000
    curr_start_ms = start_date_ms
    while curr_start_ms < end_date_ms:
        curr_end_ms = curr_start_ms + S_30_DAYS
        if curr_end_ms > end_date_ms:
            curr_end_ms = end_date_ms
        offset = 0
        while 1:
            query_param = f"from={curr_start_ms}&to={curr_end_ms}&offset={offset}&limit=100"
            sign_headers = gen_sign(gate_key, gate_secret, "GET", prefix + url, query_param)
            headers.update(sign_headers)
            r = requests.request("GET", host + prefix + url + "?" + query_param, headers=headers)
            list_temp = r.json()
            if len(list_temp) == 0:
                break
            else:
                offset = len(list_temp)
                data_list += list_temp
        curr_start_ms = curr_end_ms
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

def get_gate_withdrawals(start_dt, end_dt, gate_key, gate_secret):
    start_date_ms = int(datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    end_date_ms = int(datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S").timestamp())
    host = "https://api.gateio.ws"
    prefix = "/api/v4"
    headers = {"Accept": "application/json", "Content-Type": "application/json"}

    url = "/wallet/withdrawals"
    data_list = []
    S_30_DAYS = 2592000
    curr_start_ms = start_date_ms
    while curr_start_ms < end_date_ms:
        curr_end_ms = curr_start_ms + S_30_DAYS
        if curr_end_ms > end_date_ms:
            curr_end_ms = end_date_ms
        offset = 0
        while 1:
            query_param = f"from={curr_start_ms}&to={curr_end_ms}&offset={offset}&limit=100"
            sign_headers = gen_sign(gate_key, gate_secret, "GET", prefix + url, query_param)
            headers.update(sign_headers)
            r = requests.request("GET", host + prefix + url + "?" + query_param, headers=headers)
            list_temp = r.json()
            if len(list_temp) == 0:
                break
            else:
                offset = len(list_temp)
                data_list += list_temp
        curr_start_ms = curr_end_ms
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


def get_transfers(start_date, end_date, gate_key, gate_secret):
    df_deposit = get_gate_deposits(start_date, end_date, gate_key, gate_secret)
    df_withdrwal = get_gate_withdrawals(start_date, end_date, gate_key, gate_secret)
    df_subaccount = get_gate_subaccount_transfer(start_date, end_date, gate_key, gate_secret)
    cols = ["datetime",  "transfer", "currency", "amount"]
    df_transfer = pd.DataFrame(columns=cols)

    df_transfer = pd.concat([df_transfer, df_deposit], ignore_index=True)
    df_transfer = pd.concat([df_transfer, df_withdrwal], ignore_index=True)
    df_transfer = pd.concat([df_transfer, df_subaccount], ignore_index=True)
    
    df_transfer = df_transfer.sort_values(by=["datetime"])
    return df_transfer

def main():
    cmd_args = CmdlineParser().parse_args()
    gate_key = cmd_args.key
    gate_secret = cmd_args.secret
    start_date = cmd_args.start_date
    end_date = cmd_args.end_date
    pair = cmd_args.pair
    method = cmd_args.method
    df = pd.DataFrame()

    if method == "transfers":
        df = get_transfers(start_date, end_date, gate_key, gate_secret)
        filename = f"gate-transfer-history-{pair}-{start_date}-{end_date}.csv"
    elif method == "balance":
        df = get_gate_balance(gate_key, gate_secret)
        filename = f"gate-balance-history-{pair}-{start_date}-{end_date}.csv"
    elif method == "trades":
        df = get_gate_trades(pair, start_date, end_date, gate_key, gate_secret)
        filename = f"gate-trade-history-{pair}-{start_date}-{end_date}.csv"

    print(df)
    if  not df.empty:
        df.to_csv(filename,index=False) 

if __name__ == "__main__":
    main()