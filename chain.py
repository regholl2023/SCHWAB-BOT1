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

quote_df_lock = threading.Lock()



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


    else:
        print(f"Failed to connect with error code: {rc}")

# Callback function when a message is received
def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()

    # print(f"Received topic type:{type(topic)}, payload type:{type(payload)}")
    payload_dict = json.loads(payload)
    # print(f"01 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

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
                opt_ask = float(value['quote']['bidPrice'])

            # print(f'queried adding_to_quote_tbl2 {opt_sym}, bid:{opt_bid}, ask:{opt_ask}, last:{opt_last}')
            add_to_quote_tbl2(opt_sym, opt_bid, opt_ask, opt_last)




# Thread function to process messages from the queue
def process_message():
    global spx_last_fl

    while True:
        # Get the (topic, message) tuple from the queue
        topic, payload = message_queue.get()


        payload_dict = json.loads(payload)
        # print(f"02 Received message on topic:<{topic}> payload:\n{json.dumps(payload_dict, indent=2)}")

        if "schwab/stream" in topic:
            process_stream(topic, payload_dict)

        if "schwab/queried" in topic:
            process_queried(topic, payload_dict)





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





def persist_string(string_data):
    # Define the directory and ensure it exists
    directory = r"C:\MEIC\chain_data"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with current date and time in yymmddhhmmss format
    current_datetime = datetime.now().strftime("%y%m%d")
    filename = f"recommendations_{current_datetime}.txt"
    file_path = os.path.join(directory, filename)

    # Ensure the directory exists
    os.makedirs(directory, exist_ok=True)
    
    # Open the file in append mode, creating it if it doesn't exist, and append the string data
    with open(file_path, 'a') as file:
        file.write(string_data + '\n')



def persist_list(list_data):

    # print(f'in persist_list, list_data:\n{list_data}')

    # Define the directory and ensure it exists
    directory = r"C:\MEIC\chain_data"
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Generate the filename with current date and time in yymmddhhmmss format
    current_datetime = datetime.now().strftime("%y%m%d")
    filename = f"recommendations_{current_datetime}.txt"
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




# def display_picker_list(label_str, picked_list, persist_flag):

#     print(f'in display_picker_list(), label_str:{label_str}, persist_flag:{persist_flag}')
#     print(f'picked_list type:{type(picked_list)}, data:\n{picked_list}')
    
#     print(label_str)
#     if persist_flag == True:
#         persist_string(label_str)


 

#     # Extract headers
#     headers = picked_list[0]


#     for row in picked_list[1:]:

#         print(f'\nin display_picker_list, row type:{type(row)}, data:\n{row}\n')

#         formatted_row = {}
#         for header, value in zip(headers, row):
#             try:
#                 if header == "Short Strike":
#                     my_short_strike = int(value)
#                     formatted_row[header] = my_short_strike
#                     print(f"Short Strike: {my_short_strike}")
#                 elif header == "Long Strike":
#                     my_long_strike = int(value)
#                     formatted_row[header] = my_long_strike
#                     print(f"Long Strike: {my_long_strike}")
#                 elif header == "Net Credit":
#                     my_net_credit = float(value)
#                     formatted_row[header] = my_net_credit
#                     print(f"Net Credit: {my_net_credit:.2f}")
#                 elif header == "Short Bid":
#                     my_short_bid = float(value)
#                     formatted_row[header] = my_short_bid
#                     print(f"Short Bid: {my_short_bid:.2f}")
#                 elif header == "Long Ask":
#                     my_long_ask = float(value)
#                     formatted_row[header] = my_long_ask
#                     print(f"Long Ask: {my_long_ask:.2f}")
#                 elif header == "SPX Offset":
#                     my_spx_offset = float(value)
#                     formatted_row[header] = my_spx_offset
#                     print(f"SPX Offset: {my_spx_offset:.2f}")
#                 elif header == "Spread Width":
#                     my_spread_width = float(value)
#                     formatted_row[header] = my_spread_width
#                     print(f"Spread Width: {my_spread_width:.2f}")
#                 else:
#                     formatted_row[header] = value
#             except ValueError as e:
#                 print(f"Error processing {header} with value {value}: {e}")


#         display_str = (
#             f"spread:{my_short_strike}/{my_long_strike} "
#             f"   net:{my_net_credit:.2f} "
#             f"   width:{my_spread_width:.0f} "
#             f"   OTM offset:{my_spx_offset:.2f}"
#             f"   premiums:{my_short_bid:.2f}/{my_long_ask:.2f} "
#             )
        
#         print(f'display_picker_list display_string:<{display_str}')
        
#         print(display_str)

#         if persist_flag == True:
#             persist_string(display_str)
        


def spread_data(short_opt, long_opt, spx_price):


    try:

        if len(long_opt) < 1 or len(short_opt) < 1:
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
        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M:%S')
        print(f"1325  An error occurred: {e} at {time_str}")
        print(f"short_opt:{short_opt}")
        print(f"long_opt:{long_opt}")
        print(f'size of short_opt:{len(short_opt)}, size of long_opt:{len(long_opt)}')

    return {}  # Return an empty spread in case of an exception   



def display_spread(label_str, spread_list):

    # print(f'spread_list type:{type(spread_list)}, data:{spread_list}')

    if 'net' not in spread_list:
        disp_str = f'No net. No recommendation.'
        print(disp_str)
        persist_string(disp_str)
        return


    # print(f'in display_spread, spread_list type:{type(spread_list)}, data:\n{spread_list}')

    
    # Format the output according to the provided structure
    try:
        # Save values in float variables
        # Convert float variables to string with two decimal places
        net_fl = float(spread_list['net'])
        net_str = f"{net_fl:.2f}"

        otm_offset_fl = float(spread_list['otm_offset'])
        otm_offset_str = f"{otm_offset_fl:.2f}"

        short_bid_fl = float(spread_list['short_bid'])
        short_bid_str = f"{short_bid_fl:.2f}"

        long_ask_fl = float(spread_list['long_ask'])
        long_ask_str = f"{long_ask_fl:.2f}"


        output = (
            f"{spread_list['short_strike']}/{spread_list['long_strike']}  "
            f"net: {net_str}  "
            f"width: {spread_list['width']}  "
            f"otm offset: {otm_offset_str}  "  # Format to 2 decimal places "
            f"premiums: {short_bid_str}/{long_ask_str}"
        )
        # print(output)
    except KeyError as e:
        current_time = datetime.now()
        time_str = current_time.strftime('%H:%M:%S')
        print(f"1476 Missing key: {e} at {time_str}. Ensure all required keys are present in spread_list.")
        print(f"spread_list: {spread_list}")
        
        return
    except Exception as e:
        print(f"1225 An error occurred: {e}")
        print(f'size of spread_list:{len(spread_list)}')
        return
    
    disp_str = f'{label_str} {output}'
    print(disp_str)
    persist_string(disp_str)
    
    

def display_syms(short_opt, long_opt):

    # print(f'in display_syms, short_opt type{type(short_opt)}, data:\n{short_opt}')
    # print(f'in display_syms, long_opt type{type(long_opt)}, data:\n{long_opt}')

    short_sym = ""
    long_sym = ""

    
    # Format the output according to the provided structure
    try:


        # Save symbols into string variables
        short_sym = short_opt[0]['symbol']
        long_sym = long_opt[0]['symbol']
        output = f'{short_sym}/{long_sym}'

 
    except KeyError as e:
        print(f"Missing key: {e}. Ensure all required keys are present in the short and long option lists.")
        return
    except Exception as e:
        print(f"1225 An error occurred: {e} while trying to get symbols for short/long options")
        return
    

    disp_str = f'symbols: {output}'
    print(disp_str)
    persist_string(disp_str)


         
     

def meic_entry(schwab_client):
    global quote_df

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

        # # periodically Print the sorted DataFrame with the selected columns
        # if display_quote_throttle % 20 == 15:
        #     # print(f'\n{time_str}  SPX:{spx_last_fl}, quote_df sorted:\n{quote_df_sorted[columns_to_print]}')
        #     print(f'Total number of rows: {total_rows}')
        #     print(f'Number of rows with NaN or None in bid or ask: {rows_with_nan_bid_ask}')
        #     pass


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

            # but only if we have a minimum number of rows
            # and very few rows with None/Nan value for bid and ask

            # Total number of rows
            total_rows = len(quote_df)

            # Count the number of rows with NaN or None values in 'bid' or 'ask'
            rows_with_nan_bid_ask = quote_df[['bid', 'ask']].isna().any(axis=1).sum()


            ROW_NEEDED = 50
            MAX_NAN = 2


            if total_rows >= ROW_NEEDED and rows_with_nan_bid_ask <= MAX_NAN:
                quote_json = quote_df_sorted.to_dict(orient="records")
                pretty_json = json.dumps(quote_json, indent=4)
                # print(f'pretty_json:\n{pretty_json}')
                # current_datetime = datetime.now().strftime("%y%mdd%H%M%S")
                current_datetime = datetime.now().strftime("%y%m%d%H%M%S")
                # print(f'current_datetime:{current_datetime}')
                directory = r"C:\MEIC\chain_data"
                filename = f"quote_{current_datetime}.json"
                file_path = os.path.join(directory, filename)
                

                # Ensure the directory exists
                os.makedirs(directory, exist_ok=True)
                

                # Save the JSON data to the file with indentation for readability
                with open(file_path, "w") as file:
                    json.dump(quote_json, file, indent=4)  # Indent for human-readable formatting

                print(f'saved chain data to {file_path}')

                # print(f'quote_json type:{type(quote_json)}, data:\n{quote_json}')
                persist_string(f'\n{file_path}:')
                persist_list(quote_json)

                # Save the JSON data to the file with indentation for readability
                with open(file_path, "w") as file:
                    json.dump(quote_json, file, indent=4)  # Indent for human-readable formatting(quote_json)


                #convert quote_df_sorted to a dictionary 
                quote_sorted_json = quote_df_sorted.to_dict(orient="records")
                # print(f'\n225 quote_sorted_json type{type(quote_sorted_json)}, data:\n{quote_sorted_json}\n\n')

                # pretty_json = json.dumps(quote_sorted_json, indent=4)
                # print(f'226 pretty_json type{type(quote_sorted_json)}, data:\n{pretty_json}')

                # # print(f'calling generate_recommendation()')
                # (call_candidates, 
                # call_recommendation, 
                # put_candidates, 
                # put_recommendation, spx_price_picker) = spread_picker_B.generate_recommendation(quote_sorted_json)

                # print(f'call_candidates type:{type(call_candidates)}, data\n{call_candidates}')
                # print(f'call_recommendation type:{type(call_recommendation)}, data:\n{call_recommendation}')
                # print(f'put_candidates type:{type(put_candidates)}, data:\n{put_candidates}')
                # print(f'put_recommendation type:{type(put_recommendation)}, data:\n{put_recommendation}')


                (call_short,
                    call_long,
                    put_short,
                    put_long,
                    spx_price_picker) = recommender.generate_recommendation(quote_sorted_json)
                
                # print(f'call_short type:{type(call_short)}, data\n{call_short}')
                # print(f'call_long type:{type(call_long)}, data\n{call_long}')
                # print(f'put_short type:{type(put_short)}, data\n{put_short}')
                # print(f'put_long type:{type(put_long)}, data\n{put_long}')

                cs_len = len(call_short)
                cl_len = len(call_long)
                ps_len = len(put_short)
                pl_len = len(put_long)

                if cs_len == 0 or cl_len == 0 or ps_len == 0 or pl_len == 0:
                    print(f'at least one of the spread options could not be recommended')
                    if cs_len == 0:
                        print(f'call short was not selected')
                    if cl_len == 0:
                        print(f'call long was not selected')
                    if ps_len == 0:
                        print(f'put short was not selected')
                    if pl_len == 0:
                        print(f'put long was not selected')

                else:
                    call_spread = spread_data(call_short, call_long, spx_price_picker)
                    put_spread = spread_data(put_short, put_long, spx_price_picker)

                    print()
                    persist_string(f'\n{file_path}:')

                    display_spread("Call", call_spread)
                    display_spread(" Put", put_spread)

                    display_syms(call_short, call_long)
                    display_syms(put_short, put_long)

                    print()

            else:
                print(f'waiting for enough rows ({ROW_NEEDED}), current:{total_rows}')
                print(f'and/or fewer than {MAX_NAN} None/Nan in bid/ask, current {rows_with_nan_bid_ask}')


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
