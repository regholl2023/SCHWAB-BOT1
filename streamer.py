# streamer.py

# libraries
import schwabdev
from dotenv import load_dotenv
import os
import time
import json
import threading
from threading import Thread
from datetime import datetime, timezone, timedelta
import calendar
import paho.mqtt.client as mqtt
import sys
import pytz
import math
import logging
import keyboard
import pprint
import queue

# message queue
message_queue = queue.Queue()

# resource lock (mutex)
message_lock = threading.Lock()


def get_version():

    with open("VERSION") as f:
        return f.read().strip()
    


# Global variables
gbl_spx_price_fl = 0.0

gbl_version = ""

gbl_quit_flag = False
gbl_resubscribe_needed = False
gbl_system_error_flag = False
gbl_market_open_flag = False


call_strike_tbl = []
put_strike_tbl = []

# gbl_open_fl = None
# gbl_high_fl = None
# gbl_low_fl = None
gbl_close_fl = None
todays_epoch_time = None


# Define the MQTT broker address and port
mqtt_broker_address = "localhost"  # Use "localhost" if the broker is running on the same PC
mqtt_broker_port = 1883

def load_env_variables():
    
    # parent_dir = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    # env_file_path = os.path.join(parent_dir, '.env')
    # load_dotenv(env_file_path)

    load_dotenv()  # load environment variables from .env file

    app_key = os.getenv('MY_APP_KEY')
    secret_key = os.getenv('MY_SECRET_KEY')
    tokens_file = os.getenv('TOKENS_FILE_PATH')

    # print(f'my_local_app_key: {app_key}, my_local_secret_key: {secret_key}')
    # print(f'tokens_file type: {type(tokens_file)}, value: {tokens_file}')

    return app_key, secret_key, tokens_file

def check_for_q_key():
    # Check if the 'q' key is pressed
    if keyboard.is_pressed('q'):
        return True
    
    else:
        return False



def my_handler(message):
    message_queue.put(message)



prev_put_list = None  
prev_call_list = None     


def subscribe_to_options(passed_streamer_client): 
    global prev_put_list  
    global prev_call_list

    # print(f'streamer_client type:{type(passed_streamer_client)}, value:{passed_streamer_client}')

    my_client = passed_streamer_client

    my_client.send(my_client.level_one_equities("TSLA", "0,1,2,3,4,5,6,7,8"))

    # Extract each put symbol, create a comma-separated string, and subscribe
    put_list = ', '.join(item['Symbol'] for item in put_strike_tbl)
    # print(f'2P put_list type:{type(put_list)}, data:]\n{put_list}')
    my_client.send(my_client.level_one_options(put_list, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))


    # # Extract each call symbol, create a comma-separated string, and subscribe
    call_list = ', '.join(item['Symbol'] for item in call_strike_tbl)
    # print(f'2C call_list type:{type(call_list)}, data:]\n{call_list}')
    my_client.send(my_client.level_one_options(call_list, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))

    if put_list != prev_put_list:
        if prev_put_list != None:
            print(f'new put_list is different, new put_list:\n:{put_list}\nprev_put_list:\n{prev_put_list}')
            
        prev_put_list = put_list

    else:
        # print(f'put_list has not changed')
        pass


    if call_list != prev_call_list:
        if prev_call_list != None:
            print(f'new call_list is different, new call_list:\n:{call_list}\nprev_put_list:\n{prev_call_list}')

        prev_call_list = call_list

    else:
        # print(f'call_list has not changed')
        pass

    




def streamer_thread(client):
    global gbl_quit_flag
    global gbl_resubscribe_needed
    global gbl_market_open_flag

    while gbl_market_open_flag == False:
        # print(f'839 waiting for market open')
        time.sleep(1)


    # create the streamer client
    strm_client = client.stream


    # start the stream message handler 
    strm_client.start(my_handler)

    # subscribe to SPX (schwab api requires "$SPX")
    strm_client.send(strm_client.level_one_equities("$SPX", "0,1,2,3,4,5,6,7,8"))

    # streamer.send(streamer.level_one_equities("$SPX, TSLA, AAPL", "0,1,2,3,4,5,6,7,8"))
    # appl_keys = "AAPL  241108C00190000, AAPL  241108C00180000"
    # streamer.send(streamer.level_one_options(appl_keys, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))
    # streamer.send(streamer.level_one_equities("$SPX, TSLA", "0,1,2,3,4,5,6,7,8"))
    # streamer.send(streamer.level_one_futures("/ES", "0,1,2,3"))
    # streamer.send(streamer.level_one_options("SPXW  241122C05865000", "0,1,2,3,4,5,6,7,8")) 


    # subscribe to the desired SPXW options

    # TODO: ensure that put_strike_tbl and call_strike_tbl have data

    # print(f'put_strike_tbl type: {type(put_strike_tbl)}, data:\n{put_strike_tbl}')
    # pretty_json = json.dumps(put_strike_tbl, indent=2)
    # print(f'put_strike_tbl type:\n{pretty_json}')


    # Extract each put symbol, create a comma-separated string, and subscribe
    put_list = ', '.join(item['Symbol'] for item in put_strike_tbl)
    # print(f'1P put_list type:{type(put_list)}, data:]\n{put_list}')
    # strm_client.send(strm_client.level_one_options(put_list, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))

    # Extract each call symbol, create a comma-separated string, and subscribe
    call_list = ', '.join(item['Symbol'] for item in call_strike_tbl)
    # print(f'1C call_list type:{type(call_list)}, data:]\n{call_list}')
    # strm_client.send(strm_client.level_one_options(call_list, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))

    subscribe_to_schwab(client)
    subscribe_to_options(strm_client)


    # this loop keeps the streamer thread active
    while True:
        time.sleep(1)

        if gbl_quit_flag == True:
            print(f'streamer_thread detects quit signal')
            break

        if gbl_market_open_flag == False:
            continue

        if gbl_resubscribe_needed == True:

            print(f'in streamer thread, re-subscribe needed')
            gbl_resubscribe_needed = False
            subscribe_to_schwab(client)
            subscribe_to_options(strm_client)



    print(f'calling client.stream.stop()')
    client.stream.stop()
    print(f'returning from client.stream.stop()')

    print(f'exiting streamer_thread')



def translate_quote_key_names(json_message):
    for item in json_message['data']:
        if item['service'] == 'LEVELONE_EQUITIES':
            for content in item['content']:
                if '1' in content:
                    content['Bid Price'] = content.pop('1')
                if '2' in content:
                    content['Ask Price'] = content.pop('2')
                if '3' in content:
                    content['Last Price'] = content.pop('3')
                if '4' in content:
                    content['Bid Size'] = content.pop('4')
                if '5' in content:
                    content['Ask Size'] = content.pop('5')         
                if '6' in content:
                    content['Ask ID'] = content.pop('6')                    
                if '7' in content:
                    content['Bid ID'] = content.pop('7')  
                if '8' in content:
                    content['Total Volume'] = content.pop('8')  

        elif item['service'] == 'LEVELONE_OPTIONS':
            for content in item['content']:
                if '1' in content:
                    content['Description'] = content.pop('1')                
                if '2' in content:
                    content['Bid Price'] = content.pop('2')
                if '3' in content:
                    content['Ask Price'] = content.pop('3')
                if '4' in content:
                    content['Last Price'] = content.pop('4')
                if '5' in content:
                    content['High Price'] = content.pop('5')
                if '6' in content:
                    content['Low Price'] = content.pop('6')
                if '7' in content:
                    content['Close Price'] = content.pop('7')
                if '8' in content:
                    content['Total Volume'] = content.pop('8')
                if '10' in content:
                    content['Volatility'] = content.pop('10')
                if '28' in content:
                    content['Delta'] = content.pop('28')
                if '29' in content:
                    content['Gamma'] = content.pop('29')
                if '30' in content:
                    content['Theta'] = content.pop('30')
                if '31' in content:
                    content['Vega'] = content.pop('31')
                if '32' in content:
                    content['Rho'] = content.pop('32')


        elif item['service'] == 'LEVELONE_FUTURES':
            for content in item['content']:
                for content in item['content']:
                    if '1' in content:
                        content['Bid Price'] = content.pop('1')
                    if '2' in content:
                        content['Ask Price'] = content.pop('2')
                    if '3' in content:
                        content['Last Price'] = content.pop('3')
                    if '4' in content:
                        content['Bid Size'] = content.pop('4')
                    if '5' in content:
                        content['Ask Size'] = content.pop('5')         
                    if '6' in content:
                        content['Bid ID'] = content.pop('6')                    
                    if '7' in content:
                        content['Ask ID'] = content.pop('7')  
                    if '8' in content:
                        content['Total Volume'] = content.pop('8')  



    return(json_message)



# Create a threading lock for MQTT publishing
publish_lock = threading.Lock()

   
# Function to publish quotes via MQTT
def publish_quote(topic, payload):
    global mqtt_client_tx

    # Use the lock to ensure thread safety
    with publish_lock:
        mqtt_client_tx.publish(topic, payload)
        

        # if "000/last" in topic:


        #     # Convert the float to a string
        #     temp_last_str = str(payload)

        #     # Split the string at the decimal point
        #     parts = temp_last_str.split('.')

        #     # Determine the number of decimal places
        #     if len(parts) > 1:
        #         decimal_places = len(parts[1])
        #     else:
        #         decimal_places = 0

        #     if decimal_places > 2:
        #         print(f'034 published quote:\n  topic type{type(topic)} data:{topic}\n  payload type{type(payload)} data:{payload}')

        #         print(f'The number of decimal places in temp_last is: {decimal_places}')











def publish_levelone_equities(data):
    # pretty_json = json.dumps(data, indent=2)
    # print(f'publish_levelone_equities(), data type:{type(data)}, Pretty JSON:\n{pretty_json}')
    
    # Iterate through the items in the data list
    # for entry in data.get('data', []):
    if 'content' in data:
        for item in data['content']:
            key = item['key'].lstrip('$')  # Strip leading '$' from key if present
            key_lower = key.lower()

            if 'Bid Price' in item:
                topic = f"schwab/stock/{key_lower}/bid"
                publish_quote(topic, item['Bid Price'])
            if 'Ask Price' in item:
                topic = f"schwab/stock/{key_lower}/ask"
                publish_quote(topic, item['Ask Price'])
            if 'Last Price' in item:
                topic = f"schwab/stock/{key_lower}/last"
                publish_quote(topic, item['Last Price'])


def publish_levelone_options(data):
    # pretty_json = json.dumps(data, indent=2)
    # print(f'publish_levelone_options(), data type:{type(data)}, Pretty JSON:\n{pretty_json}')

    # Iterate through the items in the data list
    # for entry in data.get('data', []):
    if 'content' in data:
        for item in data['content']:
            key = item['key'].lstrip('$')  # Strip leading '$' from key if present
            # print(f'levelone options key type:{type(key)}, key:{key}')
            # Split the string into tokens
            tokens = key.split()
            # Check if the first token is "SPXW" and strip the trailing 'W'
            # if tokens[0] == "SPXW":
            #     tokens[0] = tokens[0].rstrip('W')

            # Join the tokens back without spaces
            key_scrub = ''.join(tokens)

            # print(f"Original key: {key}")
            # print(f"Scrubbed key: {key_scrub}")

            if 'Bid Price' in item:
                topic = f"schwab/option/spx/basic/{key_scrub}/bid"
                publish_quote(topic, item['Bid Price'])
            if 'Ask Price' in item:
                topic = f"schwab/option/spx/basic/{key_scrub}/ask"
                publish_quote(topic, item['Ask Price'])
            if 'Last Price' in item:
                topic = f"schwab/option/spx/basic/{key_scrub}/last"
                publish_quote(topic, item['Last Price'])



                temp_last = item['Last Price']

                # print(f'debugging temp_last type:{type(temp_last)}, data:{temp_last}')

                # Convert the float to a string
                temp_last_str = str(temp_last)

                # Split the string at the decimal point
                parts = temp_last_str.split('.')

                # Determine the number of decimal places
                if len(parts) > 1:
                    decimal_places = len(parts[1])
                else:
                    decimal_places = 0

                # print(f'The number of decimal places in temp_last is: {decimal_places}')

                if decimal_places > 2:
                    print(f'The number of decimal places in temp_last is: {decimal_places}')











            if 'Total Volume' in item:
                topic = f"schwab/option/spx/misc/{key_scrub}/volume"
                publish_quote(topic, item['Total Volume'])
            if 'Volatility' in item:
                topic = f"schwab/option/spx/misc/{key_scrub}/volatility"
                publish_quote(topic, item['Volatility'])
            if 'Delta' in item:
                topic = f"schwab/option/spx/greeks/{key_scrub}/delta"
                publish_quote(topic, item['Delta'])
            if 'Gamma' in item:
                topic = f"schwab/option/spx/greeks/{key_scrub}/gamma"
                publish_quote(topic, item['Gamma'])
            if 'Theta' in item:
                topic = f"schwab/option/spx/greeks/{key_scrub}/theta"
                publish_quote(topic, item['Theta'])
            if 'Vega' in item:
                topic = f"schwab/option/spx/greeks/{key_scrub}/vega"
                publish_quote(topic, item['Vega'])
            if 'Rho' in item:
                topic = f"schwab/option/spx/greeks/{key_scrub}/rho"
                publish_quote(topic, item['Rho'])
    

# determine whether this message was confirmation for an "ADD" subscriptions.
# returns True or False
def is_message_ADD(json_message):
    
    my_service = None
    my_command = None
    service_found_flag = False
    command_found_flag = False
    
    # Check for the presence of the keys "service" and "command"
    if "response" in json_message and isinstance(json_message["response"], list) and len(json_message["response"]) > 0:
        first_response = json_message["response"][0]

        # if "service" is "LEVELONE_OPTIONS"
        if "service" in first_response:
            my_service = first_response["service"]
            if my_service == "LEVELONE_OPTIONS":
                service_found_flag = True
            # print(f'service found: {my_service}')

        # if "command" is "ADD"
        if "command" in first_response:
            my_command = first_response["command"]
            if my_command == "ADD":
                command_found_flag = True
            # print(f'command found: {my_command}')

    # print(f'Service: {my_service}')
    # print(f'Command: {my_command}')

    # if this was an ADD confirmation
    if service_found_flag and command_found_flag:
        return True
    
    else:
        # print(f'this was not an ADD message. json_message type:{type(json_message)}. message data:\n{json_message}')
        # print(f'service_found_flag:{service_found_flag}. command_found_flag:{command_found_flag}')
        return False


def message_processor():
    global gbl_quit_flag
    global gbl_market_open_flag

    while gbl_market_open_flag == False:
        # print(f'294 waiting for market open')
        time.sleep(1)


    equities_cnt = 0
    options_cnt = 0


    last_message = ""
    while True:
        time.sleep(0.25)

        if gbl_market_open_flag == False:
            # print(f'930 waiting for market open')
            time.sleep(1)
            continue


        if gbl_quit_flag == True:
            print(f'exiting message_processor')
            break

        last_message = message_queue.get()


        try:
            json_message = json.loads(last_message)

            # quotes are indicated by 'data' in the message
            if 'data' in json_message:

                json_message = translate_quote_key_names(json_message)

                # pretty_json = json.dumps(json_message, indent=2)
                # print(f'after key translation, pretty json_message:\n<{pretty_json}>\n')

                for item in json_message.get("data", []):
                    service = item.get("service")
                    
                    if service == "LEVELONE_EQUITIES":
                        # print(f'LEVELONE_EQUITIES found, item:\n<{item}>\n\n')
                        # equities_cnt += 1
                        # print(f'LEVELONE_EQUITIES found {equities_cnt}')
                        publish_levelone_equities(item)
                    
                    elif service == "LEVELONE_OPTIONS":
                        # print(f'LEVELONE_OPTIONS found, item:\n<{item}>\n\n')
                        # options_cnt += 1
                        # print(f'LEVELONE_OPTIONS found {options_cnt}')
                        publish_levelone_options(item)
                        pass

            # else the message was something other than a quote
            else:

                # "ADD" messages confirm subscriptions
                if is_message_ADD(json_message):
                    # print(f'received subscription ADD confirmation')
                    pass

                else:
                    print(f'unsupported message')
                    # pretty_json = json.dumps(json_message, indent=2)
                    # print(f'unsupported message:\n{pretty_json}')
                    pass
                

        except json.JSONDecodeError as e:
            print(f'284 Error decoding JSON: {e}')


       


def extract_strike_from_sym(sym):
    # print(f'946 sym type:{type(sym)}, value:{sym}')  

    # Extract the 4-digit strike price value
    my_strike = sym[-7:-3]
    # print(f'my_strike: {my_strike}')

    return my_strike            


def build_option_tables2(option_syms, option_type):
    global put_strike_tbl
    global call_strike_tbl

    if option_type == "PUT":
        put_strike_tbl = []

    if option_type == "CALL":
        call_strike_tbl = []

    for item in option_syms:
        # print(f'294 item:<{item}>')
        strike = extract_strike_from_sym(item)
        strike_int = int(strike)
        # print(f'returned strike:{strike}, strike_int:{strike_int}')

        if option_type == "CALL":
            new_call_option = {"Type": "CALL", "Strike": strike_int, "Bid": 0.0, "Ask": 0.0, "Last": 0.0, "Symbol": item}
            # print(f'883 new call to add:{new_call_option}')
            call_strike_tbl.append(new_call_option)

        elif option_type == "PUT":
            new_put_option = {"Type": "PUT", "Strike": strike_int, "Bid": 0.0, "Ask": 0.0, "Last": 0.0, "Symbol": item}
            # print(f'884 new put to add:{new_put_option}')
            put_strike_tbl.append(new_put_option)
                    


    pass




# Build the list of call or put options to be used for subcription to the schwab api streaming quotes
def build_option_tables(option_map, option_type):
    global put_strike_tbl
    global call_strike_tbl

    # initialize the table that we are buildling

    if option_type == "PUT":
        put_strike_tbl = []

    if option_type == "CALL":
        call_strike_tbl = []


    for expiration, strikes in option_map.items():
        for strike, options in strikes.items():
            for option in options:

                # print(f'strike type:{type(strike)}')
                strike_int = int(float(strike))
                last_fl = float(option['last'])
                my_opt_symbol = option['symbol']
                my_bid = option['bid']
                my_ask = option['ask']
                # print(f"{option_type} {strike_int} Symbol: {option['symbol']}, Last: {option['last']}")
                # print(f"{option_type} {strike_int} Symbol: {option['symbol']}, Last: {last_fl}")
                # print(f'{option_type} {strike_int} Symbol: {option['symbol']}, Last: {last_fl:.2f}')
                if option_type == "CALL":
                    # print(f'this was a CALL strike') 
                    new_call_option = {"Type": "CALL", "Strike": strike, "Bid": my_bid, "Ask": my_ask, "Last": last_fl, "Symbol": my_opt_symbol}
                    # print(f'492 new_call_option:{new_call_option}')
                    call_strike_tbl.append(new_call_option)
                    # print(f'call_strike_tbl:\n{call_strike_tbl}')

                elif option_type == "PUT":
                    # print(f'this was a PUT strike') 
                    new_put_option = {"Type": "PUT", "Strike": strike, "Bid": my_bid, "Ask": my_ask, "Last": last_fl, "Symbol": my_opt_symbol}
                    # print(f'592 new_put_option:{new_put_option}')
                    put_strike_tbl.append(new_put_option)
                    # print(f'put_strike_tbl:\n{put_strike_tbl}')
                    



def is_market_open():
    global gbl_market_open_flag

    now = datetime.now(timezone.utc)
    # print(f'934 now type:{type(now)}, value:{now}')



    # Determine if the current day is Monday through Friday
    day_of_week = now.weekday()
    # Convert the integer to the corresponding weekday name 
    weekday_name = calendar.day_name[day_of_week]
    is_weekday = 0 <= day_of_week <= 4

    if is_weekday:
        # print("Today is a weekday (Monday through Friday).")
        weekday_flag = True
    else:
        # print("Today is not a weekday (Monday through Friday).")
        weekday_flag = False

    # set Eastern Time Zone
    eastern = pytz.timezone('US/Eastern')

    # Get the current time in Eastern Time
    current_time = datetime.now(eastern)

    # set markets daily start/end times
    start_time = current_time.replace(hour=9, minute=30, second=20, microsecond=0)
    end_time = current_time.replace(hour=15, minute=59, second=40, microsecond=0)


    eastern_time_str = current_time.strftime('%H:%M:%S')
        

    if weekday_flag == False or current_time < start_time or current_time > end_time:
        print(f'Market is not open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
        gbl_market_open_flag = False
        return False
    

    gbl_market_open_flag = True

    return True




def wait_for_market_to_open():


    # loop until market is open
    market_wait_cnt = 0
    while True:
        # print(f'1 wait market cnt:{market_wait_cnt}')
        market_wait_cnt += 1

        if is_market_open():
            # print(f'is_market_open() returned True, exiting wait_for_market_to_open()')
            return
        
        # print(f'is_market_open() returned False')
        
        time.sleep(20)




def get_today_in_epoch():
    # Calculate the time in milliseconds since the UNIX epoch
    now = datetime.now(timezone.utc)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    milliseconds_since_epoch = int((now - epoch).total_seconds() * 1000.0)

    # Today's date and time in milliseconds since the UNIX epoch
    # print("Today's date:", now.strftime('%Y-%m-%d %H:%M:%S %Z'))
    # print("Milliseconds since UNIX epoch:", milliseconds_since_epoch)

    return  milliseconds_since_epoch


def get_current_spx(client, milliseconds_since_epoch):


    # Loop until we have a good response for SPX price history
    while True:

        spx_history = client.price_history(
            "$SPX", 
            periodType='month',
            period=1,
            frequencyType='daily',
            frequency=1,
            # startDate='1731055026568',
            # endDate='1731055026568'
            startDate=milliseconds_since_epoch,
            endDate=milliseconds_since_epoch
            ).json()
        
        # print(f'spx_history raw type:{type(spx_history)}, data:\n{spx_history}')
        
        # pretty_json = json.dumps(spx_history, indent=2)
        # print(f'history for $SPX:\n{pretty_json}')


        # Extract the first candle data
        try:
            # first_candle = spx_history["fail"][0] # force error that candles was not found
            first_candle = spx_history["candles"][0]
            # print(f'Found First candle:\n{first_candle}')
            break

        except KeyError:
            print("Error while looking for SPX candle: 'candles' key not found in spx_history")
        except IndexError:
            print("Error while looking for SPX candle: 'candles' list is empty")
        except Exception as e:
            print(f'An unexpected error occurred while looking for SPX candle:: {e}')

        pretty_json = json.dumps(spx_history, indent=2)
        print(f'Unable to get todays candlestick for SPX!!!!!!  data:\n{pretty_json}\n\n')
        time.sleep(5)


    # Save the open, high, low, and close values as float variables
    open_fl = float(first_candle["open"])
    high_fl = float(first_candle["high"])
    low_fl = float(first_candle["low"])
    close_fl = float(first_candle["close"])

    # Print the values to verify
    # print(f'A Open: {open_fl}')
    # print(f'A High: {high_fl}')
    # print(f'A Low: {low_fl}')
    # print(f'A Close: {close_fl}')

    return open_fl, high_fl, low_fl, close_fl


# check_for_subscribe_update()
# if needed (if SPX has wandered too far since the last option subseciptiions)
# then re-subscribe
last_spx_check_time = None
def init_check_spx_last():
    global last_spx_check_time
    last_spx_check_time = datetime.now()


def check_for_subscribe_update(client):
    global last_spx_check_time
    global gbl_close_fl
    global gbl_resubscribe_needed

    CHECK_INTERVAL = 20

    # Get the current time
    current_time = datetime.now()
    elapsed_time = (current_time - last_spx_check_time).total_seconds()

    # If 30 seconds have passed since the last SPX price check
    if elapsed_time>= CHECK_INTERVAL:

        # Update the last SPX check time
        last_spx_check_time = current_time
        # print(f'Updating last_spx_check_time to {last_spx_check_time}')

        open, high, low, close = get_current_spx(client, todays_epoch_time)

        spx_change = abs(gbl_close_fl - close)
        # print(f'spx_change type:{type(spx_change)}, value:{spx_change}')
        # print(f'spx_change:{spx_change}')

        

        if spx_change >= 0.5:

            # print(f'Time to re-subscribe:{spx_change}')
            gbl_close_fl = close
            gbl_resubscribe_needed = True


            pass

    else:
        pass
        # print(f'not time for SPX check, elapsed:{elapsed_time}, Last check: {last_spx_check_time}, current time:{current_time}')



def build_strike_lists(client):
    global todays_epoch_time
    global gbl_close_fl

    strike_offset = 50


    todays_epoch_time = get_today_in_epoch()
    # print(f'todays_epoch_time type:{type(todays_epoch_time)}, value{todays_epoch_time}')


    spx_open, spx_high, spx_low, spx_close = get_current_spx(client, todays_epoch_time)
    # print(f'SPX O type:{type(spx_open)}, O/H/L/C:{spx_open}/{spx_high}/{spx_low}/{spx_close}')

    # gbl_open_fl = spx_open
    # gbl_high_fl = spx_high
    # gbl_low_fl = spx_low
    gbl_close_fl = spx_close

    # print(f'C spx_open: {spx_open}')
    # print(f'C spx_high: {spx_high}')
    # print(f'C spx_low: {spx_low}')
    # print(f'C Close: {gbl_close_fl}')


    # print(f'in build_strike_lists(), spx_low:{spx_low}, spx_high:{spx_high}')

    # Adjust spx_low and spx_high to be divisible by 5
    spx_low_strike = int(spx_low // 5 * 5)
    spx_put_low_adjusted = spx_low_strike - 150
    spx_high_strike = int((spx_high // 5 + 1) * 5)  # Adjust to the next higher strike
    spx_call_high_adjusted = spx_high_strike + 150

    # print(f'spx_put_low_adjusted:{spx_put_low_adjusted}, spx_low_strike:{spx_low_strike}, spx_high_strike:{spx_high_strike}')

    # Generate put_strikes
    put_strikes = [f"{strike:04d}" for strike in range(spx_put_low_adjusted, spx_high_strike + 5, 5)]
    # print(f'put_strikes:\n{put_strikes}')


    # print(f'spx_low_strike:{spx_low_strike}, spx_high_strike:{spx_high_strike}, spx_call_high_adjusted:{spx_call_high_adjusted}')
    
    # Generate call_strikes
    call_strikes = [f"{strike:04d}" for strike in range(spx_low_strike, spx_call_high_adjusted + 5, 5)]
    # print(f'spx_call_list:\n{call_strikes}')


    return put_strikes, call_strikes




    
def subscribe_to_schwab(client):
    pass

    # generate the list of put and call strikes to use
    put_strikes, call_strikes = build_strike_lists(client)
    # print(f'AA put_strikes type:{type(put_strikes)}, data:\n{put_strikes}')
    # print(f'AA call_strikes type:{type(call_strikes)}, data:\n{call_strikes}')

    
    # Get the current date in yymmdd format
    current_date = datetime.today().strftime('%y%m%d')
    # print(f'792 current_date type:{(type(current_date))}, value:{current_date}')

    # Create the list of put strike symbols
    put_strike_symbols = [f'SPXW  {current_date}P0{str(strike).zfill(4)}000' for strike in put_strikes]
    # print(f'put strike symbols: {put_strike_symbols}')
    build_option_tables2(put_strike_symbols, "PUT")


    # Create the list of put strike symbols
    call_strike_symbols = [f'SPXW  {current_date}C0{str(strike).zfill(4)}000' for strike in call_strikes]
    # print(f'call strike symbols: {call_strike_symbols}')
    build_option_tables2(call_strike_symbols, "CALL")



last_put_index = 9999
last_call_index = 9999

def update_quote(client):
    global put_strike_tbl
    global call_strike_tbl
    global last_put_index 
    global last_call_index 

    basic_topic = "schwab/option/spx/basic/"

    

    size_put_list = len(put_strike_tbl)
    size_call_list = len(call_strike_tbl)
    # print(f'size_put_list:{size_put_list}, size_call_list:{size_call_list}')
    # print(f'put_strike_tbl type:{type(put_strike_tbl)}, data:\n{put_strike_tbl}')

    # print(f'size_put_list type:{type(size_put_list)}, value:{size_put_list}')
    # print(f'last_put_index type:{type(last_put_index)}, value:{last_put_index}')

    # print(f'prev last_put_index:{last_put_index}, prev last_call_index:{last_call_index}, ')


    last_put_index += 1
    # print(f'incremented last_put_index:{last_put_index}')
    
    if last_put_index >= size_put_list:
        last_put_index = 0
        # print(f'223A resetting last_put_index')
    else:
        # print(f'223B incremented last_put_index:{last_put_index} is less than size_put_list:{size_put_list}')
        pass

    last_call_index += 1

    if last_call_index >= size_call_list:
        last_call_index = 0
    else:
        pass

    # print(f' new last_put_index:{last_put_index}, size_put_list:{size_put_list}')
    # print(f' new last_call_index:{last_call_index}, size_call_list:{size_call_list}')  

    if size_put_list > 0:
        current_put_item = put_strike_tbl[last_put_index]

        if 'Symbol' in current_put_item:
            put_opt_sym = current_put_item['Symbol']
            # print(f'need to get and publish quote for put_opt_sym:{put_opt_sym}')




            # put_quote = client.quote(put_opt_sym).json()

            try:
                put_quote = client.quote(put_opt_sym).json()
            except Exception as e:
                # Handle any exceptions that might occur
                print(f"client.quote(put_opt_sym): An error occurred: {e}")
                return







            # print(f'put_quote type:{type(put_quote)}, data:\n')
            # pprint.pprint(put_quote)

            # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            key = list(put_quote.keys())[0]
            # print(f'key:{key}')
            put_ask = put_quote[key]['quote']['askPrice']
            # print(f'put_ask:{put_ask}')
            put_bid = put_quote[key]['quote']['bidPrice']
            # print(f'put_bid:{put_bid}')
            put_last = put_quote[key]['quote']['closePrice']
            # print(f'put_last:{put_last}')
            put_symbol = put_quote[key]['symbol']
            # print(f'put_symbol:{put_symbol}')
            stripped_put_sym = put_symbol.replace(" ", "")
            # print(f'stripped_put_sym:{stripped_put_sym}')

            pub_topic = basic_topic + stripped_put_sym + "/bid"
            # print(f' bid pub_topic:{pub_topic}')
            publish_quote(pub_topic, put_bid)

            pub_topic = basic_topic + stripped_put_sym + "/ask"
            # print(f' ask pub_topic:{pub_topic}')
            publish_quote(pub_topic, put_ask)

            # pub_topic = basic_topic + stripped_put_sym + "/last"
            # # print(f'last pub_topic:{pub_topic}')
            # publish_quote(pub_topic, put_last)








            # # Convert the float to a string
            # temp_last_str = str(put_last)

            # # Split the string at the decimal point
            # parts = temp_last_str.split('.')

            # # Determine the number of decimal places
            # if len(parts) > 1:
            #     decimal_places = len(parts[1])
            # else:
            #     decimal_places = 0

            # if decimal_places > 2:
            #     print(f'593 published pub_topic quote:\n  topic type{type(pub_topic)} data:{pub_topic}\n  payload put_last type{type(put_last)} data:{put_last}')

            #     print(f'The number of decimal places in temp_last is: {decimal_places}')

            #     print(f'put_quote type:{type(put_quote)}, data:\n{put_quote}')


















    if size_call_list > 0:
        current_call_item = call_strike_tbl[last_call_index]

        if 'Symbol' in current_call_item:
            call_opt_sym = current_call_item['Symbol']
            # print(f'need to get and publish quote for call_opt_sym:{call_opt_sym}')




            # call_quote = client.quote(call_opt_sym).json()

            try:
                call_quote = client.quote(call_opt_sym).json()
            except Exception as e:
                # Handle any exceptions that might occur
                print(f"client.quote(call_opt_sym): An error occurred: {e}")
                return




            # print(f'call_quote type:{type(call_quote)}, data:\n')
            # pprint.pprint(call_quote)



            # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            key = list(call_quote.keys())[0]
            # print(f'key:{key}')
            call_ask = call_quote[key]['quote']['askPrice']
            # print(f'call_ask:{call_ask}')
            call_bid = call_quote[key]['quote']['bidPrice']
            # print(f'cll_bid:{call_bid}')
            call_last = call_quote[key]['quote']['closePrice']
            # print(f'call_last:{call_last}')
            call_symbol = call_quote[key]['symbol']
            # print(f'call_symbol:{call_symbol}')
            stripped_call_sym = call_symbol.replace(" ", "")
            # print(f'stripped_call_sym:{stripped_call_sym}')

            pub_topic = basic_topic + stripped_call_sym + "/bid"
            # print(f' bid pub_topic:{pub_topic}')
            publish_quote(pub_topic, call_bid)

            pub_topic = basic_topic + stripped_call_sym + "/ask"
            # print(f' ask pub_topic:{pub_topic}')
            publish_quote(pub_topic, call_ask)

            # pub_topic = basic_topic + stripped_call_sym + "/last"
            # # print(f'last pub_topic:{pub_topic}')
            # publish_quote(pub_topic, call_last)



def main():
    global gbl_quit_flag
    global mqtt_client_tx

    # global gbl_open_fl
    # global gbl_high_fl
    # global gbl_low_fl
    global gbl_close_fl
    global todays_epoch_time
    global gbl_system_error_flag
    global gbl_market_open_flag
    global gbl_version


    gbl_version = get_version()
    print(f'\nschwab-stream app version {gbl_version}\n')
    

    main_loop_seconds_count = 0

    logging.basicConfig(level=logging.INFO)


    app_key, secret_key, my_tokens_file = load_env_variables()

    # create schwabdev client
    client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)

    



    # Create an MQTT client_tx
    mqtt_client_tx = mqtt.Client()
    # print(f'mqtt_client_tx: {mqtt_client_tx}')


    # Connect to the MQTT broker
    mqtt_connect_tx_return = mqtt_client_tx.connect(mqtt_broker_address, mqtt_broker_port)
    # print(f'returned from client_tx.connection, connect_return: {mqtt_connect_tx_return}')


    # Start the streamer_thread
    streamer_thread_obj = Thread(target=streamer_thread, args=(client,), name="streamer_thread", daemon=True)
    streamer_thread_obj.start()

    # Start the message_processor thread
    message_processor_thread = Thread(target=message_processor, name="message_processor", daemon=True)
    message_processor_thread.start()



    # run for a trading session
    while True:

        gbl_system_error_flag = False

    
        wait_for_market_to_open()

        init_check_spx_last()

 
        main_loop_seconds_count = 0  
        
        # main trading session loop
        while True:

            if gbl_quit_flag == True:
                print(f'quit signal received in main()')

                print(f'waiting for streamer_thread_obj to finish')
                streamer_thread_obj.join()
                print(f'streamer_thread_obj has finished')

                print(f'waiting for message_processor_thread to finish')
                message_processor_thread.join()
                print(f'message_processor_thread has finished')
                break

            
            #main loop wait
            time.sleep(1.0)
            main_loop_seconds_count += 1

            check_for_subscribe_update(client)
            update_quote(client)

            # occasionally check to see if the market is still open
            if main_loop_seconds_count % 10 == 0:

                if is_market_open():
                    # break out of main trading session loop
                    break

            if gbl_system_error_flag == True:
                # break out of main trading session loop
                break

            else:
                pass


        if gbl_quit_flag == True:
            # break out of run for a trading session
            break





    


        


    print(f'exited mai() while loop')

    

    print("exiting main() ")
    time.sleep(1)

    keyboard.clear_all_hotkeys()
    keyboard.unhook_all()


    # Flush output buffers

    sys.stdout.flush()

    # Use sys.exit to terminate cleanly
    sys.exit(0)

if __name__ == '__main__':
    main()


