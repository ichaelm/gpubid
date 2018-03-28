import urllib.request as req
import json

CRYPTOCOMPARE_URL = 'https://min-api.cryptocompare.com'
CRYPTOCOMPARE_PRICE_PATH='/data/price'
MAX_RESP_BYTES = 10000

def query_price_usd(symbol):
    resp = req.urlopen(CRYPTOCOMPARE_URL + CRYPTOCOMPARE_PRICE_PATH
                       + '?fsym=' + symbol + '&tsyms=' + 'USD')    
    with resp:
        encoding = resp.info().get_content_charset('utf-8')
        price_dict = json.loads(resp.read().decode(encoding))
        price = None
        try:
            price = price_dict['USD']
        except KeyError:
            raise Exception('Symbol "' + symbol + '" not found')
        if price <= 0:
            raise Exception("Price is not valid")
        return price
        
