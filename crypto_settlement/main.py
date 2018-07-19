import numpy as np
import pandas as pd
import datetime as dt
import time
import requests
import logging
import sys

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


def data_retriever(prod_id='BTC-USD'):
    """
    This function will connect to GDAX api and receive live trading data of
    given product. The trade data of each product include: trade_id, time,
    price, and quantity (size).

    args:
        prod_id: product ID, eg. BTC-USD, ETH-USD

    return: new data retrieved from GDAX
    """

    url = 'https://api.gdax.com/products/{}/trades'.format(str(prod_id))
    r = requests.get(url)

    raw_data = r.json()

    # Convert to dataframe and keep desired amount of data
    df = pd.DataFrame(raw_data).set_index('trade_id')

    # Convert string to datetime object
    df['time'] = df['time'].apply(iso_converter)

    return df


def parties_assigner(n_parties, table1, table2=None):
    """
    This function will assign buyers and sellers to each trade.

    The assignment is simulated by randomly assigning the trades to two out of
    10 or more (adjustable in parameter n_parties) parties as long and short
    sides.

    args:
        n_parties: total number of counter parties
        table: the table of trades with assigned counter parties.
    """

    parties_list = []

    if table2 is not None:
        merged = table1.reset_index().merge(table2.reset_index(),
                                            indicator=True,
                                            how='outer').set_index('trade_id')
        new_rows = merged[merged['_merge'] == 'right_only'].drop('_merge',
                                                                 axis=1)

        if len(new_rows) > 0:

            print('Newly added %.2f trades.' % len(new_rows))

            for i in np.arange(len(new_rows)):
                parties_list.append(np.random.choice(n_parties, 2,
                                                     replace=False))

            parties_list = np.array(parties_list)

            new_rows['long'] = parties_list[:, 0]
            new_rows['short'] = parties_list[:, 1]

            table1 = pd.concat([table1, new_rows])

    else:
        for i in np.arange(len(table1)):
            parties_list.append(np.random.choice(n_parties, 2, replace=False))

        parties_list = np.array(parties_list)

        table1['long'] = parties_list[:, 0]
        table1['short'] = parties_list[:, 1]

    return table1.sort_index(ascending=False)


def settlement_calculator(trade_data, prod_id, window_size=3600):
    """
    This function will calculate the volume weighted average price (VWAP) in
    the moving window of given size and calculate settlement obligations of
    each party.

    The settlement price is calculated as colume-weighted average price (VWAP)
    settlement value per product, in USD. The default window size used for
    settlement price calculation is one hour (or 3600 seconds).

    The settlement obligation is the difference between the settlement price
    and latest trade price, multiplied by the quantity. Positive settlement
    obligation means the participant owes money; negative settlement means
    the participant has right to claim money.

    args:
        trade_data: the data should contain information of product, price,
        volume, time, buyer & sellers.
        prod_id: the product id, eg. BTC-USD, ETH-USD
        window_size: the moving window size used to calculate volume
        weighted average price.

    return: settlements table which has settlement information of each party.
    """

    # Take the data of lastest hour
    last_hr_data = trade_data[trade_data['time'] > dt.datetime.utcnow() -
                              dt.timedelta(seconds=window_size)]

    # Calculate worth of each trade
    last_hr_data['price'] = last_hr_data['price'].astype(float)
    last_hr_data['size'] = last_hr_data['size'].astype(float)
    last_hr_data['trade_worth'] = last_hr_data['price'] * last_hr_data['size']

    # Calculate total worth of crypto in all trade of each party
    total_worth = last_hr_data.groupby('long')['trade_worth'].sum() - \
                  last_hr_data.groupby('short')['trade_worth'].sum()

    # Calculate total quantity of crypto in all trade for each party
    total_quantity = last_hr_data.groupby('long')['size'].sum() - \
                     last_hr_data.groupby('short')['size'].sum()

    # Calculate volume-weighted average price settlement value
    settlements = pd.DataFrame(total_worth / total_quantity,
                               columns=['vwap_value'])

    # Assign currency USD
    settlements['currency'] = prod_id[-3:]

    # Insert total quantity calculated earlier
    settlements['quantity'] = total_quantity

    # Rename index
    settlements.index.names = ['party']

    # Retrieve latest trade price
    r = requests.get('https://api.gdax.com/products/{}/ticker'.format(prod_id))
    settlements['current_price'] = float(r.json()['price'])

    # Calculate settlement obligation of each party
    settlements['settlement_obligation'] = (settlements['vwap_value'] -
                                            settlements['current_price']) * \
                                           settlements['quantity']

    return settlements


def balance_calculator(bal, set, set_old=None):
    """
    Update the participants' balances in the balances table from the previous
    settlement run

    args:
        bal: the balances from last execution
        set: the settlemments from this execution
        set_old: the settlements from last execution

    return: updated balances reflecting new settlements
    """

    if set_old is None:
        bal['current_balance'] = bal['current_balance'] - \
                                 set['settlement_obligation']
    else:
        bal['current_balance'] = bal['current_balance'] - \
                                 (set['settlement_obligation'] -
                                  set_old['settlement_obligation'])

    return bal


if __name__ == '__main__':
    # Loop will run until keyboard interrupt (Ctrl-C)
    try:
        # Set initial balance for counterparties
        balances = pd.DataFrame({'party': range(10),
                                 'initial_balance': 100000,
                                 'current_balance': 100000}).set_index('party')
        start_time = time.time()

        # 1st data retrieval
        trades_BTC = data_retriever('BTC-USD')
        trades_ETH = data_retriever('ETH-USD')

        # Assign trades to counter parties
        trades_BTC = parties_assigner(10, trades_BTC)
        trades_ETH = parties_assigner(10, trades_ETH)

        # Calculate settlements
        settlements_BTC = settlement_calculator(trades_BTC, 'BTC-USD')
        settlements_ETH = settlement_calculator(trades_ETH, 'ETH-USD')

        print('The initial settlements:')
        print(settlements_BTC)
        print(settlements_ETH)

        # Update the balances with regard to BTC and ETH trades
        balances = balance_calculator(balances, settlements_BTC)
        balances = balance_calculator(balances, settlements_ETH)

        print('The initial balances:')
        print(balances)

        # Trigger execution every preset time interval
        elapsed_time = time.time() - start_time
        time.sleep(max(0, 10 - elapsed_time))

        # Iterate executions
        while True:
            # The start time for each minutely execution
            start_time = time.time()

            # Keep old settlements tables
            settlements_BTC_old = settlements_BTC.copy()
            settlements_ETH_old = settlements_ETH.copy()

            # Retrieve existing trades up to 1000
            new_trades_BTC = data_retriever('BTC-USD')
            new_trades_ETH = data_retriever('ETH-USD')

            # Assign trades to counter parties
            trades_BTC = parties_assigner(10, trades_BTC, new_trades_BTC)
            trades_ETH = parties_assigner(10, trades_ETH, new_trades_ETH)

            # Calculate settlements
            settlements_BTC = settlement_calculator(trades_BTC, 'BTC-USD')
            settlements_ETH = settlement_calculator(trades_ETH, 'ETH-USD')

            print('The updated settlements:')
            print(settlements_BTC)
            print(settlements_ETH)

            # Update the balances with regard to BTC and ETH trades
            balances = balance_calculator(balances, settlements_BTC,
                                          settlements_BTC_old)
            balances = balance_calculator(balances, settlements_ETH,
                                          settlements_ETH_old)

            print('The updated balances:')
            print(balances)

            # Trigger execution every preset time interval
            elapsed_time = time.time() - start_time
            time.sleep(max(0, 10 - elapsed_time))

    except KeyboardInterrupt:
        pass
