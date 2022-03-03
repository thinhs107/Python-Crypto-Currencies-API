import json
import requests
from Helper import ConnectPostgreSQL as sql
import json as js
from datetime import datetime, date
from datetime import timedelta


def init_requests(response_url):
    try:
        response = requests.get(response_url)
        response.close()
        return response.status_code
    except requests.exceptions.HTTPError as err:
        raise print(SystemExit(err))


def get_coin_information(coin_url):
    # Get today date to store into dic
    current_date = datetime.now().strftime("%d-%m-%Y")
    last_update_by = 'tph'

    # Get previous week
    previous_seven_week = date.today() - timedelta(7)
    previous_seven_week = previous_seven_week.strftime('%d-%m-%Y')
    # print(previous_seven_week)
    # print(previous_seven_week.strftime('%d-%m-%Y'))

    try:

        coin_response = requests.get(coin_url)
        # print(coin_response.json())
        data = js.dumps(coin_response.json(), indent=4)
        # Convert string dict to dict
        dict = json.loads(data)
        # print(dict.get['usd'])
        # Add new key values to dic
        # Get crypto since it is a key value in json
        key_list = list(dict.keys())
        for value in key_list:
            Crypto_value = (dict[value]['usd'])

        parent_url = f'https://api.coingecko.com/api/v3/coins/{key_list[0]}/history?date={current_date}&localization=en'
        history_data = get_history_data(parent_url)

        # print(history_data)

        # Format string dict to dict
        history_data = json.loads(history_data)

        # Create new Dict to store information
        new_dict = {}

        coin_response.close()
        new_dict['Crypto'] = key_list
        new_dict['Crypto_value'] = Crypto_value
        new_dict['localization'] = history_data['localization']['en']
        new_dict['thumb_image'] = history_data['image']['thumb']
        new_dict['small_image'] = history_data['image']['small']
        new_dict['market_data'] = history_data['market_data']['current_price']['usd']
        new_dict['market_cap'] = history_data['market_data']['market_cap']['usd']
        new_dict['total_vol'] = history_data['market_data']['total_volume']['usd']
        new_dict['Datetime'] = current_date
        new_dict['Last_Update_By'] = last_update_by

        # print('localization: ' + history_data['localization']['en'])
        # print(coin_url)
        # print(dict)
        # sql.crypto_insert_into_db(coin_response.json())
        # coin_response.close()

        # Insert into Tables
        Insert_statement = """INSERT INTO dbo.crypto(crypto_names, 
                                                    crypto_values, 
                                                    localization, 
                                                    thumb_image, 
                                                    small_image, 
                                                    market_data, 
                                                    market_cap, 
                                                    total_vol, 
                                                    datetime, 
                                                    last_update_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ;"""

        # Insert into PostgreSQL
        sql.conn_to_db(Insert_statement, 'pythonprojects', (key_list,
                                                            Crypto_value,
                                                            history_data['localization']['en'],
                                                            history_data['image']['thumb'],
                                                            history_data['image']['small'],
                                                            history_data['market_data']['current_price']['usd'],
                                                            history_data['market_data']['market_cap']['usd'],
                                                            history_data['market_data']['total_volume']['usd'],
                                                            current_date,
                                                            last_update_by
                                                            ))

        # print(new_dict)
    except requests.exceptions.HTTPError as err:
        raise print(SystemExit(err))


def get_coins_list():
    coins_list_response = requests.get('https://api.coingecko.com/api/v3/coins/list')
    coins_list_data = js.dumps(coins_list_response.json(), indent=2)
    # print(type(coins_list_data))
    # print(type(coins_list_data))
    coins_list_response.close()
    return coins_list_data


def get_history_data(input_url):
    try:
        print(input_url)
        historic_reponse = requests.get(input_url)
        historic_data = js.dumps(historic_reponse.json(), indent=2)

        # Close Request
        historic_reponse.close()
        return historic_data
    except requests.exceptions.HTTPError as err:
        raise print(SystemExit(err))


if __name__ == '__main__':

    # Insert Statement
    Insert_coins_list = """INSERT INTO config.coins_list (coins_list) VALUES(%s) ;"""

    # Define the lists of coins that I want research
    crypto_coins = ["binance-bitcoin",
                    "bitcoin",
                    "cardano",
                    "dogecoin",
                    "ethereum",
                    "monero",
                    "litecoin",
                    "stellar",
                    "polkadot",
                    "solana",
                    "tether"]

    # Testing
    # crypto_coins = ['bitcoin']

    url_parms = ['ping',
                 'simple/price?ids='
                 ]

    url = f'https://api.coingecko.com/api/v3/{url_parms[0]}'

    # Get coins list
    output_list_of_coins = get_coins_list()
    Dict = eval(output_list_of_coins)
    # print(Dict)
    for key in Dict:
        coin_id = key['id']
        # Insert into PostgreSQL
        # sql.conn_to_db(Insert_coins_list, "pythonprojects", coin_id)
        coin_id

    if init_requests(url) == 200:
        for coin in crypto_coins:
            get_coin_information(f'https://api.coingecko.com/api/v3/{url_parms[1]}{coin}&vs_currencies=usd')

    else:
        print('unsuccessful connection')

    print('complete')
