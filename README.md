# exchange-custom-api

How to use:

-k, --key             ----------    API key, | example : -k "abcdefghijklmnopqrstuvwxyz123"
-z, --secret          ----------    Secret keu | example : -z "abcdefghijklmnopqrstuvwxyz123"
-pp, --passphrase     ----------    passphrase for KuCoin | example : -pp "zyxwvu987654321!"
-g, --group           ----------    group number for AscendEX | example : -g "1"
-s, --start_date      ----------    start date | format : YYYY-MM-DD HH:MM:SS | example : -s "2023-01-01 00:00:00"
-e, --end_date        ----------    end date | format : YYYY-MM-DD HH:MM:SS | example : -s "2023-12-31 00:00:00"
-p, --pair            ----------    market pair | format : XXX-YYY | example : -p "XYZ-ABC"
-m, --method          ----------    method | choices : trades, balance, transfers | example : -m "trades"
-ex, --exchanges      ----------    exchanges | choices : ascendex, kucoin, gate | example : -ex "ascendex"

example:
python exchange_api.py -s "2023-01-01 00:00:00" -e "2023-12-31 00:00:00" -k "abcdefghijklmnopqrstuvwxyz123" -z "abcdefghijklmnopqrstuvwxyz123" -pp "zyxwvu987654321!" -p "XYZ-ABC" -m "trades" -ex "kucoin"