import numpy as np
import pandas as pd
import datetime as dt
import time
import requests

from PublicClient import PublicClient

pd.options.mode.chained_assignment = None


def iso_converter(s):
    """
    Convert a string to datetime object, and deal with exceptions possibly
    causing bugs.

    args:
        s: a string in iso format

    return: a datetime object
    """
    if len(s) == 20:
        t = dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    else:
        t = dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

    return t


def data_retriever(prod_id='BTC-USD', amt=100):
    """
    args:
        prod_id: product ID, eg. BTC-USD, ETH-USD
        lim: API rate limit

    return: new data retrieved from GDAX
    """

    url = 'https://api.gdax.com/products/{}/trades'.format(str(prod_id))
    r = requests.get(url)

    # Get raw data
    raw_data = public_client.get_product_trades(product_id=prod_id,
                                                limit=lim)
    print(str(len(raw_data)) + ' data points have been collected...')

    while len(raw_data) < lim:
        # Get raw data
        raw_data = public_client.get_product_trades(product_id=prod_id,
                                                    limit=lim)
        print(str(len(raw_data)) + ' data points have been collected...')
        time.sleep(0.4) # GDAX rate limit: 3 requests per second

    print('Data collection complete!')
    print('Last retrieval time (UTC): ' + str(dt.datetime.utcnow()))

    # Convert to dataframe and keep desired amount of data
    df = pd.DataFrame(raw_data).set_index('trade_id')[0:lim]

    # Convert string to datetime object
    df['time'] = df['time'].apply(iso_converter)

    return df


"""
def select_data(data, delay=20, first_exec=True):
    
    This function will select the data in accord to time window.

    args:
        data: all data points to select from
        delay: time delayed of received data
        first_exec: whether it is first execution

    return: trades within the time window
    
    # print('select_data received table:')
    # print(data)

    # Get current UTC time
    utc_now = dt.datetime.utcnow()
    # print('utc_now: '+str(utc_now))

    # Get current minute
    current_minute = dt.datetime(utc_now.year, utc_now.month, utc_now.day,
                                 utc_now.hour, utc_now.minute)

    # Get desired time window
    window_end = current_minute - dt.timedelta(seconds=delay)
    window_start = current_minute - dt.timedelta(seconds=delay+10)
    # print('window_end: '+str(window_end))
    # print('window_start: ' + str(window_start))

    # Convert string to datetime object
    data_time = data['time'].apply(iso_converter)

    # Create mask for data selection
    if first_exec:
        # If first execute, select all data prior to delay
        mask = (data_time < window_end)
    else:
        # Select data within the time window
        mask = (data_time < window_end) & (data_time > window_start)

    trades_selected = data[mask]

    return trades_selected
    """


def parties_assign(n_parties, table):
    """
    Assign buyers and sellers to each trade

    args:
        n_parties: total number of counter parties
        table: the table of trades
    """
    # print('parties_assign received table:')
    # print(table)
    parties_list = []

    for i in np.arange(len(table)):
        parties_list.append(np.random.choice(n_parties, 2, replace=False))

    parties_list = np.array(parties_list)

    table['Buyer'] = parties_list[:, 0]
    table['Seller'] = parties_list[:, 1]

    return table


def vwap_settlement(data, window=3600):
    """
    Calculate the volume weighted average price (VWAP) in the moving window of
    given size

    args:
        data: the data should contain information of product, price, volume,
        time, buyer & sellers.
        window: the moving window which the function use to calculate volume
        weighted average price.

    return: settlement table
    """
    data['trade_net_worth'] = data['price'] * data['size']

    settlements = []

    return settlements


if __name__ == '__main__':
    # Loop will run until keyboard interrupt (Ctrl-C)
    try:
        # Set initial balance for counterparties
        balances = pd.DataFrame({'Party': range(10), 'Balance': 100000}).set_index('Party')

        # 1st data retrieval
        trades_BTC = data_retriever('BTC-USD', 1000)
        trades_ETH = data_retriever('ETH-USD', 1000)

        # Assign trades to counter parties
        trades_BTC = parties_assign(10, trades_BTC)
        trades_ETH = parties_assign(10, trades_ETH)

        # Iterate executions
        while True:
            # The start time for each minutely execution
            start_time = time.time()

            # Retrieve existing trades up to 1000
            new_trades_BTC = data_retriever('BTC-USD', 1000)
            new_trades_ETH = data_retriever('ETH-USD', 1000)

            # Assign trades to counter parties
            new_trades_BTC = parties_assign(10, new_trades_BTC)
            new_trades_ETH = parties_assign(10, new_trades_ETH)

            pd.concat([trades_BTC,new_trades_BTC], ignore_index=False)
            pd.concat([trades_ETH, new_trades_ETH], ignore_index=False)


            #settlements = vwap_settlement(trades)

            # Trigger execution every preset time interval
            elapsed_time = time.time() - start_time
            time.sleep(30 - elapsed_time)

    except KeyboardInterrupt:
        pass
