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
from tabulate import tabulate
import calendar
import pytz

quote_df_lock = threading.Lock()

global mqtt_client

global gbl_total_message_count
gbl_total_message_count = 0



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





spx_last_fl = None
spx_bid_fl = None
spx_ask_fl = None

# Define a queue for inter-thread communication
message_queue = queue.Queue()

# MQTT settings
BROKER_ADDRESS = "localhost"



# MQTT mode
MQTT_MODE_RAW = 0
MQTT_MODE_TOPICS = 1
mqtt_mode = MQTT_MODE_RAW

if mqtt_mode == MQTT_MODE_TOPICS:
    SPX_LAST_TOPIC = "schwab/stock/SPX/last"
    SPX_OPT_BID_ASK_LAST_TOPIC = "schwab/option/spx/basic/#"
    SPX_OPT_BID_ASK_LAST_CHECK = "schwab/option/spx/basic/"
    SPX_SCHWAB_STREAM = ""
    SPX_SCHWAB_QUERIED = ""

elif mqtt_mode == MQTT_MODE_RAW:
    SPX_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_CHECK = ""
    SPX_SCHWAB_STREAM = "schwab/stream"
    SPX_SCHWAB_QUERIED = "schwab/queried"

else:
    SPX_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_TOPIC = ""
    SPX_OPT_BID_ASK_LAST_CHECK = ""
    SPX_SCHWAB_STREAM = ""
    SPX_SCHWAB_QUERIED = ""   


GRID_REQUEST_TOPIC = "schwab/spx/grid/request/#"
GRID_RESPONSE_TOPIC = "schwab/spx/grid/response/"

# Callback function when the client connects to the broker
def on_connect(client, userdata, flags, rc):

    if rc == 0:
        if mqtt_mode == MQTT_MODE_TOPICS:
            print("Connected to MQTT broker successfully.")
            client.subscribe(SPX_LAST_TOPIC)
            print(f"Subscribed to topic: {SPX_LAST_TOPIC}")
            client.subscribe(SPX_OPT_BID_ASK_LAST_TOPIC)
            print(f"Subscribed to topic: {SPX_OPT_BID_ASK_LAST_TOPIC}")

        elif mqtt_mode == MQTT_MODE_RAW:
            client.subscribe(SPX_SCHWAB_STREAM)
            print(f"Subscribed to topic: {SPX_SCHWAB_STREAM}")
            client.subscribe(SPX_SCHWAB_QUERIED)
            print(f"Subscribed to topic: {SPX_SCHWAB_QUERIED}")

        client.subscribe(GRID_REQUEST_TOPIC)
        print(f"Subscribed to topic: {GRID_REQUEST_TOPIC}")



    else:
        print(f"Failed to connect with error code: {rc}")

# Callback function when a message is received
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    # print(f'Received topic type:{type(topic)}, topic:<{topic}>')
    # print(f'payload type:{type(payload)},\ndata:<{payload}>')

    # try:
    #     payload_dict = json.loads(payload)
    #     print(f"01 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")
    # except Exception as e:
    #     print(f"grid manager on_message() An error occurred: {e} while trying to convert payload to json")


    # Put the topic and payload into the queue as a tuple
    message_queue.put((topic, payload))


# Create an empty pandas DataFrame for quotes
quote_df = pd.DataFrame(columns=['symbol', 'bid', 'bid_time', 'ask', 'ask_time', 'last', 'last_time'])


# Function to update the quote DataFrame
def add_to_quote_tbl(topic, payload):
    global quote_df

    # Check if the topic starts with the desired prefix
    if topic.startswith("schwab/option/spx/basic"):
        
        # Extract symbol and quote type
        parts = topic.split('/')
        symbol = parts[4]  # 5th level in topic
        quote_type = parts[5]  # Last level in topic ('bid', 'ask', 'last')
        
        # Parse payload as a float (handle invalid payloads gracefully)
        try:
            value = float(payload)
        except ValueError:
            print(f"Invalid payload: {payload} for topic: {topic}")
            return

        # Get the current time
        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M:%S:%f')[:-3]

        temp_loaded_flag = False
        temp_last = None
        temp_row_index = None
        temp_bid = None
        temp_ask = None
        temp_sym = None
        

        with quote_df_lock:    

            # Check if the symbol already exists in quote_df
            if symbol in quote_df['symbol'].values:
                # Update the existing row
                row_index = quote_df.index[quote_df['symbol'] == symbol][0]
                if quote_type == 'bid':
                    quote_df.at[row_index, 'bid'] = value
                    quote_df.at[row_index, 'bid_time'] = time_str
                elif quote_type == 'ask':
                    quote_df.at[row_index, 'ask'] = value
                    quote_df.at[row_index, 'ask_time'] = time_str
                elif quote_type == 'last':
                    temp_last = value
                    temp_row_index = row_index
                    temp_bid = quote_df.at[row_index, 'bid']
                    temp_ask = quote_df.at[row_index, 'ask']
                    temp_sym = quote_df.at[row_index, 'symbol']
                    temp_loaded_flag = True

                    quote_df.at[row_index, 'last'] = value
                    quote_df.at[row_index, 'last_time'] = time_str
            else:
                # Create a new row for the symbol
                new_row = pd.DataFrame([{
                    'symbol': symbol,
                    'bid': value if quote_type == 'bid' else None,
                    'bid_time': time_str if quote_type == 'bid' else None,
                    'ask': value if quote_type == 'ask' else None,
                    'ask_time': time_str if quote_type == 'ask' else None,
                    'last': value if quote_type == 'last' else None,
                    'last_time': time_str if quote_type == 'last' else None
                }], columns=quote_df.columns)


                with warnings.catch_warnings():
                    warnings.simplefilter(action='ignore', category=FutureWarning)
                    quote_df = pd.concat([quote_df, new_row], ignore_index=True)



        # if temp_loaded_flag == True:
        #     # print()
        #     # print(f'checking last value for {temp_sym} at row {temp_row_index} last:{temp_last} bid:{temp_bid} ask:{temp_ask}')
        #     # print(f'topic:{topic} payload:{payload}')
            
        #     # Split the string at the decimal point
        #     parts = payload.split('.')

        #     # Determine the number of decimal places
        #     if len(parts) > 1:
        #         decimal_places = len(parts[1])
        #     else:
        #         decimal_places = 0

        #     print(f'The number of decimal places in the payload is: {decimal_places}')
        #     print()




            
            
            temp_loaded_flag = False



# Function to update the quote DataFrame
def add_to_quote_tbl2(sym,bid,ask,last):
    global quote_df

    if bid == None and ask == None and last == None:
        print(f'{sym} has all None values')
        return
    

    # Get the current time
    current_time = datetime.now()
    time_str = current_time.strftime('%H:%M:%S:%f')[:-3]
    

    bid_time = time_str
    ask_time = time_str
    last_time = time_str


    temp_loaded_flag = False
    temp_last = None
    temp_row_index = None
    temp_bid = None
    temp_ask = None
    temp_sym = None

    # sym_stripped = sym.replace(" ", "")

    # print(f'add_to_quote_tbl2 sym_stripped:{sym_stripped}, bid type{type(bid)}, bid val:{bid}')
    # print(f'bid type{type(bid)}, bid val:{bid}, ask type{type(ask)}, bid val:{ask}, last type{type(last)}, last val:{last}')
    

    with quote_df_lock:  
        # symbol = sym_stripped  
        symbol = sym

        # if the symbol already exists in quote_df, we update the values
        if symbol in quote_df['symbol'].values:

            # print(f'{symbol} was found in table, updating values')

            # Update the existing row
            row_index = quote_df.index[quote_df['symbol'] == symbol][0]
            if bid != None:
                quote_df.at[row_index, 'bid'] = bid
                quote_df.at[row_index, 'bid_time'] = time_str

            if ask != None:
                quote_df.at[row_index, 'ask'] = ask
                quote_df.at[row_index, 'ask_time'] = time_str

            if last != None:
                quote_df.at[row_index, 'last'] = last
                quote_df.at[row_index, 'last_time'] = time_str
        
        # else the symbol did not exit in the table so we add a new row
        else:
            # print(f'{symbol} was not found in table, creating a new row')
            if bid == None:
                bid_time = None
            if ask == None:
                ask_time = None
            if last == None:
                last_time = None

            # Create a new row for the symbol
            new_row = pd.DataFrame([{
                'symbol': symbol,
                'bid': bid,
                'bid_time': bid_time,
                'ask': ask,
                'ask_time': ask_time,
                'last': last,
                'last_time': last_time
            }], columns=quote_df.columns)

            with warnings.catch_warnings():
                warnings.simplefilter(action='ignore', category=FutureWarning)
                quote_df = pd.concat([quote_df, new_row], ignore_index=True)
            



def process_stream(topic, payload_dict):
    global spx_last_fl
    global spx_bid_fl
    global spx_ask_fl

    equitity_key = None



    # Check for the existence of 'data'
    if "data" in payload_dict:
        # Loop through each item in 'data'
        for item in payload_dict["data"]:
            # Check for 'service' and handle accordingly
            if "service" in item:
                service = item["service"]
                if service == "LEVELONE_EQUITIES":
                    # print(f"streamed equities service: {service}")
                    # Print timestamp in Eastern time zone
                    if "timestamp" in item:
                        ts = item["timestamp"]
                        eastern = pytz.timezone("US/Eastern")
                        timestamp_dt = datetime.fromtimestamp(ts / 1000, eastern)
                        # print(f"streamed equities timestamp (ET): {timestamp_dt}")
                    # Process content
                    if "content" in item:
                        for content in item["content"]:

                            if "key" in content:
                                # print(f"streamed equities key: {content['key']}")
                                equitity_key = content['key']
                                # print(f'equitity_key type:{type(equitity_key)}, value:{equitity_key}')

                            if "bid" in content:
                                # print(f"streamed equities bid: {content['bid']}")
                                if equitity_key == "$SPX":
                                    spx_bid_fl = float(content['bid'])
                                pass
                            if "ask" in content:
                                # print(f"streamed equities ask: {content['ask']}")
                                if equitity_key == "$SPX":
                                    spx_ask_fl = float(content['ask'])
                                pass
                            if "last" in content:
                                # print(f"streamed equities last: {content['last']}")
                                if equitity_key == "$SPX":
                                    spx_last_fl = float(content['last'])
                                    # print(f'found $SPX in equitity_key:{equitity_key}, value:{spx_last_fl}')
                                    
                                    add_to_quote_tbl2(equitity_key, spx_bid_fl, spx_ask_fl, spx_last_fl)

                            


                elif service == "LEVELONE_OPTIONS":


                    # print(f"streamed options service: {service}")
                    # Print timestamp in Eastern time zone
                    if "timestamp" in item:
                        ts = item["timestamp"]
                        eastern = pytz.timezone("US/Eastern")
                        timestamp_dt = datetime.fromtimestamp(ts / 1000, eastern)
                        # print(f"streamed options timestamp (ET): {timestamp_dt}")
                    # Process content
                    if "content" in item:


                        for content in item["content"]:

                            opt_sym = None
                            opt_bid = None
                            opt_ask = None
                            opt_last = None

                            if "key" in content:
                                # print(f"streamed options key: {content['key']}")
                                opt_sym = content['key']
                            if "bid" in content:
                                # print(f"streamed options bid: {content['bid']}")
                                opt_bid = content['bid']
                            if "ask" in content:
                                # print(f"streamed options ask: {content['ask']}")
                                opt_ask = content['ask']
                            if "last" in content:
                                # print(f"streamed options last: {content['last']}")
                                opt_last = content['last']

                            if opt_sym != None:
                                if opt_bid == None and opt_ask == None and opt_last == None:
                                    # print(f'{opt_sym} has all None values 2, payload_dict:\n{payload_dict}')
                                    pass

                                else:

                                    # print(f'opt_bid type:{type(opt_bid)}, value:{opt_bid}')
                                    # print(f'opt_ask type:{type(opt_ask)}, value:{opt_ask}')

                                    # if opt_ask == 0.05:
                                    #     print(f'opt_ask is 0.05, opt_bid:{opt_bid}')


                                    # print(f'streamed adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
                                    add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)

                             

def process_queried(topic, payload_dict):
    global spx_last_fl


    # print(f"process_queried() topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

    # Iterate through the items in payload_dict
    for key, value in payload_dict.items():
        if "assetMainType" in value and value["assetMainType"] == "OPTION":
            opt_sym = None
            opt_bid = None
            opt_ask = None
            opt_last = None

            # print(f"Processing queried item: {key}")
            
            
            # Check and print symbol
            if "symbol" in value:
                # print(f" queried Symbol: {value['symbol']}")
                opt_sym = value['symbol']
            
            # Check and print quote[bidPrice]
            if "quote" in value and "bidPrice" in value["quote"]:
                # print(f"queried Bid Price: {value['quote']['bidPrice']}")
                opt_bid = float(value['quote']['bidPrice'])
            
            # Check and print quote[askPrice]
            if "quote" in value and "askPrice" in value["quote"]:
                # print(f" queried Ask Price: {value['quote']['askPrice']}")
                opt_ask = float(value['quote']['askPrice'])

            # print(f'queried adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
            add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)




# Thread function to process messages from the queue
def process_message():
    global spx_last_fl
    global gbl_total_message_count

    while True:
        # Get the (topic, message) tuple from the queue
        topic, payload = message_queue.get()

        gbl_total_message_count += 1


        # payload_dict = json.loads(payload)
        # print(f"02 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

        # payload may be empty or may not be json data
        try:
            # Attempt to parse the JSON data
            payload_dict = json.loads(payload)

        except json.JSONDecodeError:
            # print("Payload is not valid JSON")
            payload_dict = {}

        except Exception as e:
            print(f"8394 An error occurred: {e} while trying load json data")
            payload_dict = {}
  


        if "schwab/stream" in topic:
            process_stream(topic, payload_dict)

        elif "schwab/queried" in topic:
            process_queried(topic, payload_dict)

        elif "schwab/spx/grid/request" in topic:
            print(f'grid request topic type:{type(topic)}, topic:<{topic}>')
            # Parse the topic to extract the last level
            request_id = topic.split('/')[-1]

            # Print the result to confirm
            # print(f'request_id:<{request_id}>')


            with quote_df_lock: 
                quote_df_sorted = quote_df.sort_values(by='symbol')

    
            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df_sorted[['bid', 'ask']].isna().any(axis=1).sum()

            publish_grid(quote_df_sorted, rows_with_nan_bid_ask, request_id)

            pass



        # Process the message (print in this example)
        
        # print(f'Processing message.  topic:<{topic}>, payload:<{payload}>')
        # print(f'topic type:{type(topic)}, topic payload:{type(payload)}')

        if "stock/SPX/last" in topic:
            spx_last_fl = float(payload)
            # print(f'new SPX value:{spx_last_fl}')

        if SPX_OPT_BID_ASK_LAST_CHECK in topic:
            # print(f'recieved SPX option bid/ask/last topic')
            add_to_quote_tbl(topic, payload)



        time.sleep(0.1)


# Function to publish grid via MQTT
def publish_grid(quote_df_sorted, rows_with_nan_bid_ask, request_id):
    global mqtt_client


    my_topic = GRID_RESPONSE_TOPIC + request_id

    # print(f'in publish_grid, rows_with_nan_bid_ask:{rows_with_nan_bid_ask}')
    print(f'grid manager publishing topic:<{my_topic}>')

    # Force publishing of an empty json list
    # rows_with_nan_bid_ask = 1

    if rows_with_nan_bid_ask > 0:
        json_data = json.dumps([])

    else:
        json_data = quote_df_sorted.to_json(orient='records')

    mqtt_client.publish(my_topic, json_data)

    # # Load JSON string into a Python dictionary for pretty printing
    # parsed_json = json.loads(json_data)
    # print(f'grid data:\n{json.dumps(parsed_json, indent=4)}')









def meic_entry(schwab_client):
    global quote_df
    no_none_nan_flag = False    # indicates when all bid and ask have values

    display_quote_throttle = 0

    while True:
        time.sleep(1)
        display_quote_throttle += 1

        # print(f'display_quote_throttle:{display_quote_throttle}')
        # print('meic_entry loop')

        

        pd.set_option('display.max_rows', None)
        with quote_df_lock: 


            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df[['bid', 'ask']].isna().any(axis=1).sum()

            # Total number of rows
            total_rows = len(quote_df)

            # Sort the DataFrame by the 'symbol' column
            quote_df_sorted = quote_df.sort_values(by='symbol')



        # Select the columns you want to print
        columns_to_print = ['symbol', 'bid', 'ask', 'last']

        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M')

        # periodically Print the sorted DataFrame with the selected columns
        if display_quote_throttle % 10 == 8:

            if rows_with_nan_bid_ask > 0:
                print(f'# rows:{total_rows}, # Nan/None in bid/ask:{rows_with_nan_bid_ask}, # messages:{gbl_total_message_count}')
                no_none_nan_flag = False

            else:
                if no_none_nan_flag == False:
                    print(f'All bid and ask now have quoted values')
                    no_none_nan_flag = True



            # print(f'\n{time_str}  SPX:{spx_last_fl}, grid:\n{quote_df_sorted[columns_to_print]}')


            # print(f'Total number of rows: {total_rows}, message count: {gbl_total_message_count}')
            # print(f'Number of rows with NaN or None in bid or ask: {rows_with_nan_bid_ask}')
            # print(f'# rows:{total_rows}, # Nan/None in bid/ask:{rows_with_nan_bid_ask}, # messages:{gbl_total_message_count}')
            # publish_grid(quote_df_sorted, rows_with_nan_bid_ask, "dummy_req_id")
            pass


        # periodically save the sorted dataframe to a .json file
        if display_quote_throttle % 60 == 5:
            # quote_json = quote_df_sorted.to_dict(orient="records")
            # pretty_json = json.dumps(quote_json, indent=4)
            # # print(f'pretty_json:\n{pretty_json}')
            # # current_datetime = datetime.now().strftime("%y%mdd%H%M%S")
            # current_datetime = datetime.now().strftime("%y%m%d%H%M%S")
            # print(f'current_datetime:{current_datetime}')
            # directory = r"C:\MEIC\chain_data"
            # filename = f"quote_{current_datetime}.json"
            # file_path = os.path.join(directory, filename)
            
            # # Ensure the directory exists
            # os.makedirs(directory, exist_ok=True)
            
            # # Save the JSON data to the file with indentation for readability
            # with open(file_path, "w") as file:
            #     json.dump(quote_json, file, indent=4)  # Indent for human-readable formatting

            # print(f'saved chain data to {file_path}')

            pass
                

            

        # periodically call the spread recommender
        quote_interval_modulo = 60
        quote_interval_remainder = quote_interval_modulo - 13
        quote_interval_warning = quote_interval_remainder - 2
        
        if display_quote_throttle % quote_interval_modulo == quote_interval_warning:
            # print(f'\n\n*************** quote in 2 seconds ******************\n\n')
            pass

        if display_quote_throttle % quote_interval_modulo == quote_interval_remainder:

            pass


            # # Total number of rows
            # total_rows = len(quote_df)

            # # Count the number of rows with NaN or None values in 'bid' or 'ask'
            # rows_with_nan_bid_ask = quote_df[['bid', 'ask']].isna().any(axis=1).sum()

            # ROW_NEEDED = 50
            # MAX_NAN = 2

            # if total_rows >= ROW_NEEDED and rows_with_nan_bid_ask <= MAX_NAN:
            #     quote_json = quote_df_sorted.to_dict(orient="records")

            
            # else:
            #     pass
            #     # print(f'waiting for enough rows ({ROW_NEEDED}), current:{total_rows}')
            #     # print(f'and/or fewer than {MAX_NAN} None/Nan in bid/ask, current {rows_with_nan_bid_ask}')


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
