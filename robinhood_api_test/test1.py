from Robinhood import Robinhood

# https://github.com/Jamonek/Robinhood
my_trader = Robinhood()
logged_in = my_trader.login(username="nathanzhang3", password="WOSHIxue8#*")

# Get stock information
stock_instrument = my_trader.instruments("AAPL")[0]
print(stock_instrument)

# Get a stock's quote
quote_info = my_trader.quote_data("AAPL")
print(quote_info)

# buy_order = my_trader.place_buy_order(stock_instrument, 1)
# sell_order = my_trader.place_sell_order(stock_instrument, 1)