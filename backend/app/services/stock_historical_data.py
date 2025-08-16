from pykrx import stock

kospi_ticker_list = stock.get_market_ticker_list(market="KOSPI")
kosdaq_ticker_list = stock.get_market_ticker_list(market="KOSDAQ")

print(len(kospi_ticker_list))
print(len(kosdaq_ticker_list))
kr_stocks = kospi_ticker_list + kosdaq_ticker_list
kr_stocks = list(set(kr_stocks))
print(len(kr_stocks))
print(kr_stocks[:10])
