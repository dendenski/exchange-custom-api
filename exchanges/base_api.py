from datetime import datetime, timedelta

class BaseApi:
    def __init__(self, input_data, **kwargs):
        self._api_key = input_data["_api_key"]
        self._secret_key = input_data["_secret_key"]
        self._passphrase_key = input_data["_passphrase_key"]
        self._start_date = input_data["_start_date"]
        self._end_date = input_data["_end_date"]
        self._pair = input_data["_pair"]
        self._method = input_data["_method"]
        self._exchange = input_data["_exchange"]
        self._group = input_data["_group"]
        self.start_date_s = int(datetime.strptime(input_data["_start_date"], "%Y-%m-%d %H:%M:%S").timestamp())
        self.end_date_s = int(datetime.strptime(input_data["_end_date"], "%Y-%m-%d %H:%M:%S").timestamp())
        self.start_date_ms = int(datetime.strptime(input_data["_start_date"], "%Y-%m-%d %H:%M:%S").timestamp()*1000)
        self.end_date_ms = int(datetime.strptime(input_data["_end_date"], "%Y-%m-%d %H:%M:%S").timestamp()*1000)

    def get_data(self):
        methods = {
            "balance" : self.get_balance,
            "trades" : self.get_trades,
            "transfers" : self.get_transfers,
        }
        if self._method in methods:
            return methods[self._method]()
        else:
            print("no implementation")

    def get_balance(self):
        print("no implementation")

    def get_trades(self):
        print("no implementation")

    def get_transfers(self):
        print("no implementation")

        

    
