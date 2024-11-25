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


# Get the streamer version number from the file VERSIONS
def get_version():
    return_str = "None"
    try:
        with open("VERSION") as f:
            return_str = f.read().strip()
    except FileNotFoundError:
        return_str = "Error: VERSION file does not exist"
    except OSError as e:
        return_str = f"Error: An OS error occurred: {e}"

    except Exception as e:
        return_str = f"Error: An unexpected error occurred: {e}"

    return return_str
    


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

PUBLISH_MODE_RAW = 0
PUBLISH_MODE_TOPICS = 1
publish_mode = PUBLISH_MODE_RAW

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
    global gbl_system_error_flag

    # print(f'streamer_client type:{type(passed_streamer_client)}, value:{passed_streamer_client}')

    my_client = passed_streamer_client

    # my_client.send(my_client.level_one_equities("TSLA", "0,1,2,3,4,5,6,7,8"))

    # Extract each put symbol, create a comma-separated string, and subscribe
    put_list = ', '.join(item['Symbol'] for item in put_strike_tbl)
    # print(f'2P put_list type:{type(put_list)}, data:]\n{put_list}')

    try:
        my_client.send(my_client.level_one_options(put_list, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))
    
    except Exception as e:
        print(f"100SEF my_client.send for put_list: An error occurred: {e}")
        gbl_system_error_flag = True
        return



    # # Extract each call symbol, create a comma-separated string, and subscribe
    call_list = ', '.join(item['Symbol'] for item in call_strike_tbl)
    # print(f'2C call_list type:{type(call_list)}, data:]\n{call_list}')


    try:
        my_client.send(my_client.level_one_options(call_list, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))

    except Exception as e:
        print(f"102SEF my_client.send for call_list: An error occurred: {e}")
        gbl_system_error_flag = True
        return

    if put_list != prev_put_list:
        if prev_put_list != None:
            pass
            # print(f'new put_list is different, new put_list:\n:{put_list}\nprev_put_list:\n{prev_put_list}')
            
        prev_put_list = put_list

    else:
        # print(f'put_list has not changed')
        pass


    if call_list != prev_call_list:
        if prev_call_list != None:
            pass
            # print(f'new call_list is different, new call_list:\n:{call_list}\nprev_put_list:\n{prev_call_list}')

        prev_call_list = call_list

    else:
        # print(f'call_list has not changed')
        pass

    


def streamer_thread(client):
    global gbl_quit_flag
    global gbl_system_error_flag
    global gbl_resubscribe_needed
    global gbl_market_open_flag

    print(f'streamer_thread() entry')
    
    while gbl_market_open_flag == False:
        # print(f'839 waiting for market open')
        time.sleep(1)


    # print(f'in streamer_thread(), market is open')

    subscribe_to_schwab(client)
    if gbl_system_error_flag == True:
        print(f'in streamer_thread() startup, gbl_system_error_flag is True')
        return


    # create the streamer client

    strm_client = None

    try:
        strm_client = client.stream

    except Exception as e:
        print(f"104SEF strm_client = client.stream: An error occurred: {e}")
        gbl_system_error_flag = True
        return


    # start the stream message handler
    try: 
        strm_client.start(my_handler)

    except Exception as e:
        print(f"106SEF strm_client.start(): An error occurred: {e}")
        gbl_system_error_flag = True
        return



    # subscribe to SPX (schwab api requires "$SPX")
    try:
        strm_client.send(strm_client.level_one_equities("$SPX", "0,1,2,3,4,5,6,7,8"))

    except Exception as e:
        print(f"108SEF strm_client.send(strm_client.level_one_equities($SPX): An error occurred: {e}")
        gbl_system_error_flag = True
        return


    # streamer.send(streamer.level_one_equities("$SPX, TSLA, AAPL", "0,1,2,3,4,5,6,7,8"))
    # appl_keys = "AAPL  241108C00190000, AAPL  241108C00180000"
    # streamer.send(streamer.level_one_options(appl_keys, "0,1,2,3,4,5,6,7,8,10,28,29,30,31,32"))
    # streamer.send(streamer.level_one_equities("$SPX, TSLA", "0,1,2,3,4,5,6,7,8"))
    # streamer.send(streamer.level_one_futures("/ES", "0,1,2,3"))
    # streamer.send(streamer.level_one_options("SPXW  241122C05865000", "0,1,2,3,4,5,6,7,8")) 


    # subscribe to the desired SPXW options

    # TODO: ensure that put_strike_tbl and call_strike_tbl have data
    wait_for_strike_tbl_count = 0
    while True:
        time.sleep(1)
        put_len = len(put_strike_tbl)
        call_len = len(call_strike_tbl)

        if put_len == 0 or call_len == 0:
            print(f'waiting for put_strike_table, len:{put_len}, and call_strike_tbl, len:{call_len}, count:{wait_for_strike_tbl_count}')
            continue

        else:
            break


    # if not put_strike_tbl:
    #     temp_str = f'put_strike_table was empty'
    #     logging.error(temp_str)
    #     print(temp_str)
    #     return
    
    # if not call_strike_tbl:
    #     temp_str = f'call_strike_table was empty'
    #     logging.error(temp_str)
    #     print(temp_str)
    #     return

    

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

        # we abort if the market is no longer open
        if is_market_open() == False:
            gbl_market_open_flag = False

        abort_flag, abort_reason = any_abort_condition()

        if abort_flag == True:
            print(f'streamer_thread detects abort, reason: {abort_reason}')
            break


        if gbl_resubscribe_needed == True:

            # print(f'in streamer thread, re-subscribe needed')
            gbl_resubscribe_needed = False
            subscribe_to_schwab(client)
            subscribe_to_options(strm_client)


    print(f'streamer_thread client.stream.stop()')

    try:
        client.stream.stop()

    except Exception as e:
        print(f"110SEF client.stream.stop(): An error occurred: {e}")
        gbl_system_error_flag = True


    print(f'exiting streamer_thread')



def translate_quote_key_names(json_message):
    for item in json_message['data']:
        if item['service'] == 'LEVELONE_EQUITIES':
            for content in item['content']:
                if '1' in content:
                    content['bid'] = content.pop('1')
                if '2' in content:
                    content['ask'] = content.pop('2')
                if '3' in content:
                    content['last'] = content.pop('3')
                if '4' in content:
                    content['bid size'] = content.pop('4')
                if '5' in content:
                    content['ask size'] = content.pop('5')         
                if '6' in content:
                    content['ask ID'] = content.pop('6')                    
                if '7' in content:
                    content['bid ID'] = content.pop('7')  
                if '8' in content:
                    content['total volume'] = content.pop('8')  

        elif item['service'] == 'LEVELONE_OPTIONS':
            for content in item['content']:
                if '1' in content:
                    content['description'] = content.pop('1')                
                if '2' in content:
                    content['bid'] = content.pop('2')
                if '3' in content:
                    content['ask'] = content.pop('3')
                if '4' in content:
                    content['last'] = content.pop('4')
                if '5' in content:
                    content['high'] = content.pop('5')
                if '6' in content:
                    content['low'] = content.pop('6')
                if '7' in content:
                    content['close'] = content.pop('7')
                if '8' in content:
                    content['total volume'] = content.pop('8')
                if '10' in content:
                    content['volatility'] = content.pop('10')
                if '28' in content:
                    content['delta'] = content.pop('28')
                if '29' in content:
                    content['gamma'] = content.pop('29')
                if '30' in content:
                    content['theta'] = content.pop('30')
                if '31' in content:
                    content['vega'] = content.pop('31')
                if '32' in content:
                    content['rho'] = content.pop('32')


        elif item['service'] == 'LEVELONE_FUTURES':
            for content in item['content']:
                for content in item['content']:
                    if '1' in content:
                        content['bid'] = content.pop('1')
                    if '2' in content:
                        content['ask'] = content.pop('2')
                    if '3' in content:
                        content['price'] = content.pop('3')
                    if '4' in content:
                        content['bid size'] = content.pop('4')
                    if '5' in content:
                        content['ask size'] = content.pop('5')         
                    if '6' in content:
                        content['bid ID'] = content.pop('6')                    
                    if '7' in content:
                        content['ask ID'] = content.pop('7')  
                    if '8' in content:
                        content['total volume'] = content.pop('8')  



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




def publish_raw_queried_quote(data):

    # pretty_json = json.dumps(data, indent=2)
    # print(f'in publish_raw_streamed_quote, pretty_json type:{type(pretty_json)}, data:\n{pretty_json}')

    json_str = json.dumps(data)
    # print(f'in publish_raw_streamed_quote, json_str type:{type(json_str)}, data:\n{json_str}')


    topic = "schwab/queried"
    publish_quote(topic, json_str)
    pass



def publish_raw_streamed_quote(data):

    json_str = json.dumps(data)
    # print(f'in publish_raw_streamed_quote, json_str type:{type(json_str)}, data:\n{json_str}')

    topic = "schwab/stream"
    publish_quote(topic, json_str)
    pass


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
    

# determine whether this message is a heartbeat.  Return True/False along with 
# the timestamp of the heartbeat.
def is_heartbeat(json_message):

    heartbeat_flag = False
    heartbeat_time = None

    if "notify" in json_message:
        notify_list = json_message["notify"]
        if isinstance(notify_list, list) and len(notify_list) > 0:
            # print(f'notify is in json_message')
            for entry in notify_list:
                if "heartbeat" in entry:
                    # print(f'heartbeat is in json_message')
                    heartbeat_flag = True
                    # Get the heartbeat value
                    heartbeat_epoch = entry["heartbeat"]
                    try:
                        # Convert epoch string to integer and then to datetime
                        heartbeat_time = datetime.fromtimestamp(int(heartbeat_epoch) / 1000)
                        # heartbeat_str = heartbeat_time.strftime("%Y-%m-%d %H:%M:%S")
                        # print(f"Heartbeat datetime: {heartbeat_str}")
                    except ValueError as e:
                        print(f"Error converting heartbeat: {e}")

    return heartbeat_flag, heartbeat_time
        





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


def any_abort_condition():

    return_val = False
    reason_str = ""

    if gbl_quit_flag == True:
        reason_str += f' quit flag detected'
        return_val = True
        
    if gbl_system_error_flag == True:
        reason_str += f'error flag detected'
        return_val = True

    if gbl_market_open_flag == False:
        reason_str += f'market close detected'
        return_val = True

    return return_val, reason_str



processor_msg_cnt = 0
def message_processor():
    global gbl_quit_flag
    global gbl_system_error_flag
    global gbl_market_open_flag
    global processor_msg_cnt

    print(f'message_processor() entry')


    while gbl_market_open_flag == False:
        # print(f'294 waiting for market open')
        time.sleep(1)


    # print(f'in message_processor() when market is open')


    equities_cnt = 0
    options_cnt = 0
    msg_cnt = 0


    last_message = ""

    while True:

        abort_flag, abort_msg = any_abort_condition()

        if abort_flag == True:
            print(f'in message_processor() 1, aborting because {abort_msg}')
            return


        try:
            last_message = message_queue.get(timeout=1)  # 1 second timeout

        except queue.Empty: 

            # there was a timeout because of no message
            # we check to see if there is any reason to abort the message processor thread
            abort_flag, abort_msg = any_abort_condition()

            if abort_flag == True:
                print(f'in message_processor() 2, aborting because {abort_msg}')
                return


        # if we fall throught to the point we have a message from the queue

        # ignore LOGIN and ADD messages

        if "ADD command succeeded" in last_message:
            # print(f'got ADD command succeeded message')
            continue

        if "LOGIN" in last_message:
            # print(f'got LOGIN message')
            continue

        if "LOGOUT" in last_message:
            # print(f'got LOGOUT message')
            continue

        if "heartbeat" in last_message:
            # print(f'got heatbeat message')
            continue


        json_message = None

        try:
            json_message = json.loads(last_message)

            # pretty_json = json.dumps(json_message, indent=2)
            # print(f'raw json_message:\n{pretty_json}')

            # quotes are indicated by 'data' in the message
            if 'data' in json_message:

                json_message = translate_quote_key_names(json_message)
                # pretty_json = json.dumps(json_message, indent=2)
                # print(f'after key translation, pretty json_message:\n<{pretty_json}>\n')

                publish_raw_streamed_quote(json_message)


                # for item in json_message.get("data", []):
                #     service = item.get("service")
                    
                #     if service == "LEVELONE_EQUITIES":
                #         # print(f'LEVELONE_EQUITIES found, item:\n<{item}>\n\n')
                #         # equities_cnt += 1
                #         # print(f'LEVELONE_EQUITIES found {equities_cnt}')
                #         publish_levelone_equities(item)
                    
                #     elif service == "LEVELONE_OPTIONS":
                #         # print(f'LEVELONE_OPTIONS found, item:\n<{item}>\n\n')
                #         # options_cnt += 1
                #         # print(f'LEVELONE_OPTIONS found {options_cnt}')
                #         publish_levelone_options(item)
                #         pass

            # else the message was something other than a quote or one of the other messages trapped above

            else:
                print(f'unsupported message')
                pretty_json = json.dumps(json_message, indent=2)
                print(f'unsupported message:\n{pretty_json}')
                pass
                

        except json.JSONDecodeError as e:

            # temp_str = f'284 message json.loads error, e type:{type(e)}, value:{e}'
            # print(temp_str)

            if json_message == None:
                # we ignore json_message == None
                pass

            else:
                temp_str = f'286 message json.loads error, e type:{type(e)}, value:{e}'
                print(temp_str)
                pass

            

        time.sleep(0.001)


    

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
        # print(f'in build_option_tables2(), type:{option_type}, zeroed out put_strike_tbl')

    if option_type == "CALL":
        call_strike_tbl = []
        # print(f'in build_option_tables2(), type:{option_type}, zeroed out call_strike_tbl')

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



    # if option_type == "CALL":
    #     print(f'end of build_option_tables2(), type:{option_type}, call_strike_tbl len:{len(call_strike_tbl)}')


    # if option_type == "PUT":
    #     print(f'end of build_option_tables2(), type:{option_type}, put_strike_tbl len:{len(put_strike_tbl)}')


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
                    

def get_eastern_weekday_time():

    eastern = pytz.timezone('US/Eastern')

    eastern_time = datetime.now(eastern)
    day_of_week_str = eastern_time.strftime('%A')
    date_str = eastern_time.strftime('%m/%d/%y')

    current_time = datetime.now(eastern)
    eastern_time_str = current_time.strftime('%H:%M:%S')
    return_str = f'{day_of_week_str} {date_str} {eastern_time_str} (Eastern)'

    return return_str

    






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

    start_time = current_time.replace(hour=9, minute=30, second=10, microsecond=0)
    end_time = current_time.replace(hour=15, minute=59, second=50, microsecond=0)

    # eastern_time_str = current_time.strftime('%H:%M:%S')
    # end_time_str = end_time.strftime('%H:%M:%S')
 

    if weekday_flag == False or current_time < start_time or current_time > end_time:
        # print(f'Market is not open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
        gbl_market_open_flag = False
        return False
    
    # print(f'Market IS open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
    

    gbl_market_open_flag = True

    return True




def wait_for_market_to_open():

    global gbl_quit_flag
    global gbl_system_error_flag

    SECONDS_BETWEEN_CHECKS = 5
    ITERATIONS_BETWEEN_DISPLAY = int(60 / SECONDS_BETWEEN_CHECKS)


    # loop until market is open
    market_wait_cnt = 0
    throttle_time_display = ITERATIONS_BETWEEN_DISPLAY
    while True:
        # print(f'1 wait market cnt:{market_wait_cnt}')
        market_wait_cnt += 1

        if is_market_open():
            # print(f'is_market_open() returned True, exiting wait_for_market_to_open()')
            print(f'Market is open. Current: {get_eastern_weekday_time()}')
            return
        
        throttle_time_display += 1
        if throttle_time_display >= ITERATIONS_BETWEEN_DISPLAY:
            print(f'Market is not open. Current: {get_eastern_weekday_time()}')
            throttle_time_display = 0
        
        
        time.sleep(SECONDS_BETWEEN_CHECKS)

        if gbl_quit_flag == True or gbl_system_error_flag == True:
            return




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
    global gbl_system_error_flag

    open_fl = None
    high_fl = None
    low_fl = None
    close_fl = None

    


    # Loop until we have a good response for SPX price history
    while True:

        spx_history = None
        retry_count = 0

        try:
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
            
        except Exception as e:
            print(f"112SEF spx_history = client.price_history: An error occurred: {e}")


            if spx_history == None:
                retry_count += 1
                if retry_count > 4:
                    temp_str = f"202SEF expired retries in get_current_spx()"
                    print(temp_str)
                    logging.error(temp_str)

                    return None, None, None, None
                
                time.sleep(0.25)
                print(f"113SEF retrying") 
                continue

            gbl_system_error_flag = True
            return None, None, None, None
        
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
            temp_str = f"114SEF Error while looking for SPX candle: 'candles' key not found in spx_history"
            print(temp_str)
            pretty_json = json.dumps(spx_history, indent=2)
            print(f'spx_history:\n{pretty_json}\n\n')
            logging.error(temp_str)
            gbl_system_error_flag = True
            return open_fl, high_fl, low_fl, close_fl
        
        except IndexError:
            temp_str = f"116SEF Error while looking for SPX candle: 'candles' list is empty"
            print(temp_str)
            pretty_json = json.dumps(spx_history, indent=2)
            print(f'spx_history:\n{pretty_json}\n\n')
            logging.error(temp_str)
            gbl_system_error_flag = True
            return open_fl, high_fl, low_fl, close_fl

        except Exception as e:
            temp_str = f'118SEF An unexpected error occurred while looking for SPX candle: {e}'
            print(temp_str)
            pretty_json = json.dumps(spx_history, indent=2)
            print(f'spx_history:\n{pretty_json}\n\n')
            logging.error(temp_str)
            gbl_system_error_flag = True
            return open_fl, high_fl, low_fl, close_fl



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
    global gbl_system_error_flag

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

        if open == None or high == None or low == None or close == None:
            temp_str = f"120SEF in check_for_subscribe_update() get_current_spx returned one or more 'None' value(s)"
            print(temp_str)
            logging.error(temp_str)
            gbl_system_error_flag = True
            return


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
    global gbl_system_error_flag


    strike_offset = 50


    todays_epoch_time = get_today_in_epoch()
    # print(f'todays_epoch_time type:{type(todays_epoch_time)}, value{todays_epoch_time}')


    spx_open, spx_high, spx_low, spx_close = get_current_spx(client, todays_epoch_time)

    if spx_open == None or spx_high == None or spx_low == None or spx_close == None:
        temp_str = f"122SEF in build_strike_lists() get_current_spx returned one or more 'None' value(s)"
        print(temp_str)
        logging.error(temp_str)
        gbl_system_error_flag = True
        return None, None



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



    # generate the list of put and call strikes to use
    put_strikes, call_strikes = build_strike_lists(client)

    if put_strikes == None or call_strikes == None:
        return
    

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
    global gbl_system_error_flag

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



            put_quote = None

            try:
                put_quote = client.quote(put_opt_sym).json()
                
            except Exception as e:
                if put_quote == None:
                    # return without flagging a system error
                    # print(f'put_quote is None, returning wihtout flagging a system error')
                    return
                
                gbl_system_error_flag = True
                return
            
            # print(f'dbg client.quote 20: put_quote type:{type(put_quote)}, data:\n{put_quote}')

            
            publish_raw_queried_quote(put_quote)


            # print(f'put_quote type:{type(put_quote)}, data:\n')
            # pprint.pprint(put_quote)

            # # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            # # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            # key = list(put_quote.keys())[0]
            # # print(f'key:{key}')
            # put_ask = put_quote[key]['quote']['askPrice']
            # # print(f'put_ask:{put_ask}')
            # put_bid = put_quote[key]['quote']['bidPrice']
            # # print(f'put_bid:{put_bid}')
            # put_last = put_quote[key]['quote']['closePrice']
            # # print(f'put_last:{put_last}')
            # put_symbol = put_quote[key]['symbol']
            # # print(f'put_symbol:{put_symbol}')
            # stripped_put_sym = put_symbol.replace(" ", "")
            # # print(f'stripped_put_sym:{stripped_put_sym}')

            # pub_topic = basic_topic + stripped_put_sym + "/bid"
            # # print(f' bid pub_topic:{pub_topic}')
            # publish_quote(pub_topic, put_bid)

            # pub_topic = basic_topic + stripped_put_sym + "/ask"
            # # print(f' ask pub_topic:{pub_topic}')
            # publish_quote(pub_topic, put_ask)

            # # pub_topic = basic_topic + stripped_put_sym + "/last"
            # # # print(f'last pub_topic:{pub_topic}')
            # # publish_quote(pub_topic, put_last)





    if size_call_list > 0:
        current_call_item = call_strike_tbl[last_call_index]

        if 'Symbol' in current_call_item:
            call_opt_sym = current_call_item['Symbol']
            # print(f'need to get and publish quote for call_opt_sym:{call_opt_sym}')


            call_quote = None

            try:
                call_quote = client.quote(call_opt_sym).json()
                
            except Exception as e:
                if call_quote == None:
                    # return without flagging a system error
                    # print(f'call_quote is None, returning wihtout flagging a system error')
                    return


                print(f'dbg client.quote 30: call_quote type:{type(call_quote)}, data:\n{call_quote}')
                gbl_system_error_flag = True
                return
            
            # print(f'dbg client.quote 40: call_quote type:{type(call_quote)}, data:\n{call_quote}')

            
            publish_raw_queried_quote(call_quote)
            

            # print(f'call_quote type:{type(call_quote)}, data:\n')
            # pprint.pprint(call_quote)



            # # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            # # Save 'askPrice', 'bidPrice', and 'closePrice' in separate variables
            # key = list(call_quote.keys())[0]
            # # print(f'key:{key}')
            # call_ask = call_quote[key]['quote']['askPrice']
            # # print(f'call_ask:{call_ask}')
            # call_bid = call_quote[key]['quote']['bidPrice']
            # # print(f'cll_bid:{call_bid}')
            # call_last = call_quote[key]['quote']['closePrice']
            # # print(f'call_last:{call_last}')
            # call_symbol = call_quote[key]['symbol']
            # # print(f'call_symbol:{call_symbol}')
            # stripped_call_sym = call_symbol.replace(" ", "")
            # # print(f'stripped_call_sym:{stripped_call_sym}')

            # pub_topic = basic_topic + stripped_call_sym + "/bid"
            # # print(f' bid pub_topic:{pub_topic}')
            # publish_quote(pub_topic, call_bid)

            # pub_topic = basic_topic + stripped_call_sym + "/ask"
            # # print(f' ask pub_topic:{pub_topic}')
            # publish_quote(pub_topic, call_ask)

            # # pub_topic = basic_topic + stripped_call_sym + "/last"
            # # # print(f'last pub_topic:{pub_topic}')
            # # publish_quote(pub_topic, call_last)



def mqtt_on_connect(client, userdata, flags, reason_code, properties=None):
    global gbl_system_error_flag

    if reason_code == 0:
        print("MQTT client connected")

    else:
        print(f"128SEF MQTT client Failed to connect, return code {reason_code}")
        gbl_system_error_flag = True



def system_loop():
    global gbl_quit_flag
    global gbl_system_error_flag
    global mqtt_client_tx

    # global gbl_open_fl
    # global gbl_high_fl
    # global gbl_low_fl
    global gbl_close_fl
    global todays_epoch_time
    global gbl_system_error_flag
    global gbl_market_open_flag
    global gbl_version


    gbl_system_error_flag = False


    gbl_version = get_version()
    temp_str = f'schwab-stream app version {gbl_version}'
    print(f'\n{temp_str}\n')
    logging.info(temp_str)


    # print(f'835-100 system_loop(), entering wait for market to open')

    wait_for_market_to_open()
    if gbl_quit_flag == True or gbl_system_error_flag == True:
        return
    
    # print(f'835-105 system_loop(), market is now open')
    

    app_key, secret_key, my_tokens_file = load_env_variables()

    # create schwabdev client
    create_client_count = 0


    main_loop_seconds_count = 0
    # while we try to create a schwabdev client
    while True:
        try:
            client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)
            break

        except Exception as e:
            create_client_count += 1
            print(f"client = schwabdev.Client: An error occurred: {e}.  Attempt # {create_client_count}. Retrying")
            time.sleep(1)
            continue
    # while we try to create a schwabdev client

    # print(f'835-110 system_loop(), schwabdev client created')

    print(f"schwabdev client created")

    

    # print(f'835-120 system_loop(), attemting to create mqtt client')

    # Create an MQTT client_tx
    try:
        # mqtt_client_tx = mqtt.Client()
        mqtt_client_tx = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    except Exception as e:
        print(f"mqtt_client_tx = mqtt.Client(): An error occurred: {e}")
        return
    
    # print(f'mqtt_client_tx: {mqtt_client_tx}')

    # print(f'835-130 system_loop(), created mqtt client')

    print("MQTT client created")

    # print(f'835-140 system_loop(), attempting mqtt connection')

    # Connect to the MQTT broker
    try:
        # mqtt_connect_tx_return = mqtt_client_tx.connect(mqtt_broker_address, mqtt_broker_port)
        mqtt_client_tx.connect(mqtt_broker_address, mqtt_broker_port)

    except Exception as e:
        print(f"mqtt_client_tx.connect(): An error occurred: {e}")
        return
    
    # print(f'835-150 system_loop(), mqtt connected')

    print("MQTT client connected")

    # print(f'returned from client_tx.connection, connect_return: {mqtt_connect_tx_return}')

    # print(f'835-160 system_loop(), creating threads')

    # Start the streamer_thread
    streamer_thread_obj = Thread(target=streamer_thread, args=(client,), name="streamer_thread", daemon=True)
    streamer_thread_obj.start()

    # Start the message_processor thread
    message_processor_thread = Thread(target=message_processor, name="message_processor", daemon=True)
    message_processor_thread.start()


    # print(f'835-170 system_loop(), created threads')


    init_check_spx_last()



    # print(f'835-180 system_loop(), entering main while loop')

    # run while the market is open and while we have no system errors or quite signal
    while True:


        abort_flag, abort_reason = any_abort_condition()

        # if we have a reason to abort
        if abort_flag == True:
            temp_str = f'In system_loop, abort detected becasue {abort_reason }'
            print(temp_str)
            logging.info(temp_str)

            print(f'waiting for streamer_thread_obj to finish')
            streamer_thread_obj.join()
            print(f'streamer_thread_obj has finished')

            print(f'waiting for message_processor_thread to finish')
            message_processor_thread.join()
            print(f'message_processor_thread has finished')

            # graceful exit for mqtt client
            temp_str = f'MQTT client disconnect and loop_stop'
            print(temp_str)
            logging.info(temp_str)
            mqtt_client_tx.disconnect()  # Disconnect from broker
            mqtt_client_tx.loop_stop()  # Stop network loop

            return
        
        # if we have a system error or quit flag

            
        #main loop wait
        time.sleep(1.0)
        
        check_for_subscribe_update(client)
        update_quote(client)


        # # force quit/error
        # force_quit_count += 1
        # if force_quit_count >= 13:

        #     # temp_str = f'forcing gbl_quit_flag True'
        #     # logging.info(temp_str)
        #     # print(temp_str)
        #     # gbl_quit_flag = True

        #     temp_str = f'130SEF forcing gbl_system_error_flag True'
        #     logging.info(temp_str)
        #     print(temp_str)
        #     gbl_system_error_flag = True

        # else:
        #     print(f'force_count:{force_quit_count}')


    # run while the market is open and while we have no system errors or quite signal




main_loop_count = 0
def main():
    global gbl_quit_flag
    global gbl_system_error_flag
    global main_loop_count

    # logging.basicConfig(level=logging.INFO)
    logging.basicConfig(filename='streamer.log', level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s')

    main_loop_count 
    while True:
        main_loop_count += 1

        gbl_system_error_flag = False

        # system_loop 
        # - waits for the market to open
        # - creates clients and connections with schwab developer and with mqtt
        # - creates the various threads of the streamer
        # - monitors gbl_gbl_system_error_flag and if ever set, shuts everthing down gracefully and returns
        
        system_loop()

        if gbl_quit_flag == True:
            break

        # the only rease we should get here is if gbl_gbl_system_error_flag had been set
        # we will now loop around and call system_loop() again


    temp_str = "exiting schwab-stream"
    print(f'\n{temp_str}\n')
    logging.info(temp_str)
    time.sleep(0.525)

    # Use sys.exit to terminate cleanly
    sys.exit(0)


if __name__ == '__main__':
    main()


