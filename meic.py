import threading
import queue
import paho.mqtt.client as mqtt
import time
import os
from dotenv import load_dotenv
import schwabdev
import pandas as pd
from datetime import datetime, timezone, timedelta
import warnings
import json
import pytz
# import spread_picker_B
import recommender
from tabulate import tabulate
import calendar
import pytz
import random

quote_df_lock = threading.Lock()

global mqtt_client

global gbl_total_message_count
gbl_total_message_count = 0

global prev_request_id
prev_request_id = ""

global gbl_round_trip_start



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




# Define a queue for inter-thread communication
message_queue = queue.Queue()

# MQTT settings
BROKER_ADDRESS = "localhost"



GRID_PUB_REQUEST_TOPIC = "schwab/spx/grid/request/"
GRID_SUB_RESPONSE_TOPIC = "schwab/spx/grid/response/#"

# Callback function when the client connects to the broker
def on_connect(client, userdata, flags, rc):

    if rc == 0:
        print("current_grid_tester: Connected to MQTT broker.")
        client.subscribe(GRID_SUB_RESPONSE_TOPIC)
        print(f"current_grid_tester: Subscribed to topic: {GRID_SUB_RESPONSE_TOPIC}")


    else:
        print(f"Failed to connect with error code: {rc}")

# Callback function when a message is received
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    # print(f"grid tester: Received topic type:{type(topic)}, payload type:{type(payload)}")


    # payload may be empty or may not be json data
    try:
        # Attempt to parse the JSON data
        payload_dict = json.loads(payload)

    except json.JSONDecodeError:
        print("meic Payload is not valid JSON")
        return

    except Exception as e:
        print(f"meic An error occurred: {e} while trying load json data")
        return
        

    # print(f"01 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

    # Put the topic and payload into the queue as a tuple
    message_queue.put((topic, payload))





def persist_string(string_data):
    # Define the directory and ensure it exists
    directory = r"C:\MEIC\tranche"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with current date and time in yymmddhhmmss format
    current_datetime = datetime.now().strftime("%y%m%d")
    filename = f"tranches_{current_datetime}.txt"
    file_path = os.path.join(directory, filename)

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    
    # Open the file in append mode, creating it if it doesn't exist, and append the string data
    with open(file_path, 'a') as file:
        file.write(string_data + '\n')



def persist_list(list_data):

    # print(f'in persist_list, list_data:\n{list_data}')

    # Define the directory and ensure it exists
    directory = r"C:\MEIC\tranche"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with current date and time in yymmddhhmmss format
    current_datetime = datetime.now().strftime("%y%m%d")
    filename = f"tranches_{current_datetime}.txt"
    file_path = os.path.join(directory, filename)

    try:
    
        with open(file_path, "a") as file:
            # json.dump(list_data, file, indent=4)  # Indent for human-readable formatting

            for item in list_data:
                # Check if the required keys are present

                if 'symbol' in item and 'bid' in item and 'ask' in item:

                    bid_fl = float(item['bid'])
                    bid_str = f"{bid_fl:.2f}"
                    ask_fl = float(item['ask'])
                    ask_str = f"{ask_fl:.2f}"
                    last_fl = None
                    last_str = "None"

                    # if bid is 0 we are not interested in preserving the data
                    if bid_fl == 0.0 or bid_fl >= 10:
                        continue
                    

                    # Format the string
                    formatted_string = f"symbol:{item['symbol']}, bid:{bid_str}, ask:{ask_str}"
                    if 'last' in item:
                        last_fl = float(item['last'])
                        last_str = f"{last_fl:.2f}"
                        formatted_string += f", last:{last_str}"

                    if "SPXW24" in formatted_string:


                        # FIX ME

                        # Strip characters 8th through 17th, 19th, and 24th through 26th
                        revised_string = (
                            formatted_string[:7] +  # Keep everything up to the 7th character
                            formatted_string[17:18] +  # Keep the 18th character
                            formatted_string[19:23] +  # Keep characters 20th through 23rd
                            formatted_string[26:]  # Keep everything from the 27th character onwards
                        )

                        print("Original string:", formatted_string)
                        print("Revised string:", revised_string)
                        formatted_string = revised_string



                    file.write(formatted_string + '\n')

    except Exception as e:
        print(f"Error persist_list(): {e}")





def spread_data(short_opt, long_opt, spx_price):

    


    try:

        if len(long_opt) < 1 or len(short_opt)< 1:
            return {}
        
        # Extract data from the single-item lists
        short_opt = short_opt[0]
        long_opt = long_opt[0]

        # Retrieve necessary values
        short_symbol = short_opt['symbol']
        short_strike = short_opt['STRIKE']
        long_symbol = long_opt['symbol']
        long_strike = long_opt['STRIKE']
        short_bid = short_opt['bid']
        long_ask = long_opt['ask']

        # Calculate required values
        net = abs(short_opt['bid'] - long_opt['ask'])
        width = abs(short_strike - long_strike)
        otm_offset = abs(spx_price - short_strike)

        # Create the spread list with the required keys
        spread = {
            'short_symbol': short_symbol,
            'short_strike': short_strike,
            'long_symbol': long_symbol,
            'long_strike': long_strike,
            'net': net,
            'width': width,
            'otm_offset': otm_offset,
            'short_bid' : short_bid,
            'long_ask' : long_ask
        }


        return spread

    except KeyError as e:
        print(f"Missing key: {e}. Ensure 'short_opt' and 'long_opt' have 'symbol', 'bid', 'ask', and 'STRIKE' keys.")
    except Exception as e:
        print(f"1325  An error occurred: {e}")
        print(f'size of short_opt:{len(short_opt)}, size of long_opt:{len(long_opt)},')

    return {}  # Return an empty spread in case of an exception   



def get_syms(short_opt, long_opt):

    # print(f'in display_syms, short_opt type{type(short_opt)}, data:\n{short_opt}')
    # print(f'in display_syms, long_opt type{type(long_opt)}, data:\n{long_opt}')

    short_sym = ""
    long_sym = ""

    
    # Format the output according to the provided structure
    try:


        # Save symbols into string variables
        short_sym = short_opt[0]['symbol']
        long_sym = long_opt[0]['symbol']


 
    except KeyError as e:
        print(f"Missing key: {e}. Ensure all required keys are present in the short and long option lists.")
        return "", ""
    except Exception as e:
        print(f"1325 An error occurred: {e} while trying to get symbols for short/long options")
        return "", ""
    

    return short_sym, long_sym



def display_spread(label_str, spread_list):


    # print(f'spread_list type:{type(spread_list)}, data:{spread_list}')

    if 'net' not in spread_list:
        disp_str = f'{label_str}:  No recommendation.'
        print(disp_str)
        # persist_string(disp_str)
        return

    
    # Format the output according to the provided structure
    try:
        net_fl = float(spread_list['net'])
        net_str = f"{net_fl:.2f}"
        short_bid_fl = float(spread_list['short_bid'])
        short_bid_str = f"{short_bid_fl:.2f}"
        long_ask_fl = float(spread_list['long_ask'])
        long_ask_str = f"{long_ask_fl:.2f}"


        output = (
            f"{spread_list['short_strike']}/{spread_list['long_strike']}  "
            f"net: {net_str}  "
            f"width: {spread_list['width']}  "
            f"otm offset: {spread_list['otm_offset']:.2f}  "  # Format to 2 decimal places "
            f"premiums: {short_bid_str}/{long_ask_str}"
        )
        # print(output)
    except KeyError as e:
        print(f"Missing key: {e}. 2863 Ensure all required keys are present in spread_list.")
        return
    except Exception as e:
        print(f"1225 An error occurred: {e}")
        print(f'size of spread_list:{len(spread_list)}')
        return

    
    disp_str = f'{label_str} {output}'
    print(disp_str)
    persist_string(disp_str)
         


# Thread function to process messages from the queue
def process_message():
    global spx_last_fl
    global gbl_total_message_count

    while True:
        # Get the (topic, message) tuple from the queue
        topic, payload = message_queue.get()

        gbl_total_message_count += 1


        payload_dict = json.loads(payload)
        # print(f"02 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")


        if "schwab/spx/grid/response" in topic:
            
            request_id = topic.split('/')[-1]

            if "meic" in request_id:
                if request_id != prev_request_id:
                    print(f'Request ID mismatch!!! prev_request_id:<{prev_request_id}>, received request_id:<{request_id}>')
                    time.sleep(0.01)
                    continue

            end_time = datetime.now()
            current_time = end_time.strftime('%H:%M:%S')
            elapsed_time = end_time - gbl_round_trip_start
            # Extract the total milliseconds from the elapsed time
            elapsed_milliseconds = int(elapsed_time.total_seconds() * 1000)

            # Print the elapsed time in milliseconds
            display_str = f'\nNew tranche attempt at {current_time} Pacific Time.  Elapsed grid request/response time: {elapsed_milliseconds} mS'
            print(display_str)
            persist_string(display_str)

            # print(f'grid responose topic:<{topic}>, payload_dict type{type(payload_dict)}, data:\n{payload_dict}')
            
            (call_short,
                call_long,
                put_short,
                put_long,
                spx_price) = recommender.generate_recommendation(payload_dict)
            
            call_spread = spread_data(call_short, call_long, spx_price)
            put_spread = spread_data(put_short, put_long, spx_price)

            display_spread("Call", call_spread)
            display_spread(" Put", put_spread)


            if 'net' in call_spread or 'net' in put_spread:
                display_str = "Symbols:"
                persist_string(display_str)
                print(display_str)


            if 'net' in call_spread:
                call_short_sym, call_long_sym = get_syms(call_short, call_long)
                call_syms = f'{call_short_sym}/{call_long_sym}'
                call_syms_display = "Call " + call_syms
                persist_string(call_syms_display)
                print(call_syms_display)

            if 'net' in put_spread:
                put_short_sym, put_long_sym = get_syms(put_short, put_long)
                put_syms = f'{put_short_sym}/{put_long_sym}'
                put_syms_display = " Put " + put_syms
                persist_string(put_syms_display)
                print(put_syms_display)



            
            
 
            pass

        else:
            print(f'received unexpected topic:<{topic}>, payload_dict type{type(payload_dict)}, data:\n{payload_dict}')
            pass



        time.sleep(0.1)



def publish_grid_request():
    global mqtt_client
    global prev_request_id
    global gbl_round_trip_start


    topic = GRID_PUB_REQUEST_TOPIC 

    # Get the current time
    current_time = datetime.now()

    # Format the time as hhmmssmmmm, where "mmmm" is milliseconds
    req_id = "meic" + current_time.strftime('%H%M%S') + f"{current_time.microsecond // 1000:04d}"


    topic = topic + req_id

    # print(f'publishing topic:<{topic}>')

    mqtt_client.publish(topic, " ")

    gbl_round_trip_start = datetime.now()



    prev_request_id = req_id

    pass




def meic_entry(schwab_client):
    global quote_df

    display_quote_throttle = 0

    while True:
        time.sleep(1)
        display_quote_throttle += 1


        # periodically Print the sorted DataFrame with the selected columns
        if display_quote_throttle % 20 == 15:
            # print(f'in meic_entry, caling publish_grid_request()')
            publish_grid_request()
            pass


def is_market_open():
    # global gbl_market_open_flag

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
        # gbl_market_open_flag = False
        return False
    
    # print(f'Market IS open.  Current day of week: {weekday_name}.  Current eastern time: {eastern_time_str}')
    

    # gbl_market_open_flag = True

    return True




# Main function to set up MQTT client and start the processing thread
def main():
    global mqtt_client


    throttle_wait_display = 0

    print(f'chain: waiting for market to open')

    while True:
        if is_market_open():
            break

        throttle_wait_display += 1
        # print(f'throttle_wait_display: {throttle_wait_display}')
        if throttle_wait_display % 3 == 2:

            eastern = pytz.timezone('US/Eastern')
            current_time = datetime.now(eastern)
            eastern_time_str = current_time.strftime('%H:%M:%S')

            print(f'waiting for market to open, current East time: {eastern_time_str}')

            pass


        time.sleep(10)

    print(f'chain: market is open')


    # Initialize MQTT client
    mqtt_client = mqtt.Client()
    # mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    # Assign callback functions
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    # Connect to the MQTT broker
    print("Connecting to MQTT broker...")
    mqtt_client.connect(BROKER_ADDRESS)

    app_key, secret_key, my_tokens_file = load_env_variables()

    # create schwabdev client
    schwab_client = schwabdev.Client(app_key, secret_key, tokens_file=my_tokens_file)


    # Start the keyboard thread
    # keboard_thread = threading.Thread(target=keyboard_handler_task, name="keyboard_handler_task")
    # keboard_thread.daemon = True  # Daemonize thread to exit with the main program
    # keboard_thread.start()



    # Start the message processing thread
    processing_thread = threading.Thread(target=process_message, name="process_message")
    processing_thread.daemon = True  # Daemonize thread to exit with the main program
    processing_thread.start()

    # Start the meic_entry thread
    # meic_entry_thread = threading.Thread(target=meic_entry, name="meic_entry")
    meic_entry_thread = threading.Thread(target=meic_entry, name="meic_entry", args=(schwab_client,))

    meic_entry_thread.daemon = True  # Daemonize thread to exit with the main program
    meic_entry_thread.start()

    # Start the MQTT client loop (handles reconnects and message callbacks)
    mqtt_client.loop_forever()




# Entry point of the program
if __name__ == "__main__":
    print(f'chain: calling main')
    main()
    print(f'chain: returned from main')
