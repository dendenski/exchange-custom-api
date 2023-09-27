import argparse
import pandas as pd
import numpy as np

from exchanges.ascendex_api import AscendexApi
from exchanges.base_api import BaseApi
from exchanges.bitmart_api import BitmartApi
from exchanges.bybit_api import BybitApi
from exchanges.gate_api import GateApi
from exchanges.kucoin_api import KucoinApi

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
            help="Market pair format XRD_USDT"
        )
        self.add_argument(
            "-m", 
            "--method", 
            type=str, 
            default="", 
            help="Select method from trades, transfers, balance"
        )
        self.add_argument(
            "-ex", 
            "--exchange", 
            type=str, 
            default="", 
            help="Select exchange from gate, kucoin, mexc"
        )
        self.add_argument(
            "-g", 
            "--group", 
            type=str, 
            default="", 
            help="ascendex group number"
        )

def main():
    cmd_args = CmdlineParser().parse_args()
    ex = cmd_args.exchange

    input_data = {
        "_api_key" : cmd_args.key,
        "_secret_key" : cmd_args.secret,
        "_passphrase_key" : cmd_args.passphrase,
        "_start_date" : cmd_args.start_date,
        "_end_date" : cmd_args.end_date,
        "_pair" : cmd_args.pair,
        "_method" : cmd_args.method,
        "_exchange" : cmd_args.exchange,
        "_group" : cmd_args.group,
    }
    ex_list = {
            "ascendex" : AscendexApi,
            "bitmart" : BitmartApi,
            "bybit" : BybitApi,
            "gate" : GateApi,
            "kucoin" : KucoinApi,
            "base" : BaseApi
        }

    ex_api = ex_list[ex](input_data) if ex in ex_list else ex_list["base"](input_data)
    df = pd.DataFrame()
    df = ex_api.get_data()
    print(df)
    # if  not df.empty:
    #     df.to_csv(filename,index=False) 

if __name__ == "__main__":
    main()