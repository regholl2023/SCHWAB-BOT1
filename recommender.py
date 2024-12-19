# strike_picker
#
# 1. Evaluates SPX put or call options grid data and 
#   generates a list of all vertical spread candidates that satisfy rules.
#
# 2. From the list of candidates, selects the spread with optimal credit
#
# calls_grid_candidates and puts_grid_candidates are called with a dataframe that contains the call or put
# grid data in a pandas dataframe formatted as shown in the following example
# 
# -------------------------------------------
#    STRIKE   BID   ASK
# 0    5590   0.1  0.15
# 1    5595   0.1  0.15
# 2    5600   0.1  0.15
# 3    5605   0.1  0.15
# 4    5610  0.15   0.2
# 5    5615  0.15   0.2
# 6    5620   0.2  0.25
# 7    5625   0.2  0.25
# 8    5630  0.25   0.3
# 9    5635  0.25   0.3
# 10   5640   0.3  0.35
# 11   5645  0.35   0.4
# 12   5650   0.4  0.45
# 13   5655   0.5  0.55
# 14   5660  0.55   0.6
# 15   5665  0.65   0.7
# 16   5670  0.75  0.85
# 17   5675   0.9  0.95
# 18   5680   1.1  1.15
# 19   5685   1.3   1.4
# 20   5690   1.6  1.65
# 21   5695  1.95   2.0
# 22   5700   2.4  2.45
# 23   5705  2.95   3.0
# 24   5710   3.6   3.7
# 25   5715   4.5   4.6
# 26   5720   5.5   5.7
# 27   5725   6.9   7.0
# 28   5730   8.5   8.7
# 29   5735  10.5  10.7
# ---------------------------------------------------------------
#
# Returns both the list of candidate spreads and the selected spread



import os
import csv
import pandas as pd
import numpy as np
from io import StringIO
import picker_config
import json
import math


# fetch the spread leg selection rules from the picker_config.py file
grid_directory = picker_config.GRID_FILES_DIRECTORY # location of option grid data files
strategy = picker_config.PICKER_STRATEGY
strategy_desc = picker_config.PICKER_STRATEGY_DESC
max_width = picker_config.MAX_SPREAD_WIDTH      # max spread width
min_width = picker_config.MIN_SPREAD_WIDTH      # min spread width
max_net_credit = picker_config.MAX_NET_CREDIT   # max net credit
min_net_credit = picker_config.MIN_NET_CREDIT   # min net credit
optimal_net_credit = picker_config.OPTIMAL_NET_CREDIT   # optimal credit
max_long_val = picker_config.MAX_LONG_VAL       # max ask value for long leg
min_long_val = picker_config.MIN_LONG_VAL       # min ask value for long leg
min_spx_distance = picker_config.MIN_SPX_DISTANCE   # minimum difference bewteen strike price of the long leg 
                                                    # and current price of SPX

max_short_target = picker_config.MAX_SHORT_TARGET
min_short_target = picker_config.MIN_SHORT_TARGET

# print(f'grid_directory type:{type(grid_directory)}, value: <{grid_directory}>')
# print(f'max_width type:{type(max_width)}, value: {max_width}')
# print(f'min_width type:{type(min_width)}, value: {min_width}')
# print(f'max_net_credit:{type(max_net_credit)}, value: {max_net_credit}')
# print(f'min_net_credit:{type(min_net_credit)}, value: {min_net_credit}')
# print(f'max_long_val:{type(max_long_val)}, value: {max_long_val}')
# print(f'min_long_val:{type(min_long_val)}, value: {min_long_val}')
# print(f'min_spx_distance:{type(min_spx_distance)}, value: {min_spx_distance}')
# print(f'strategy: {strategy} ({strategy_desc})')


def puts_picker_optimal_net(put_tbl_pd, spx_last_fl):

    put_selection_df = pd.DataFrame() # empty dataframe
    put_candidates_df = pd.DataFrame() # empty dataframe

    DEBUG_CANDIDATE_ANALYIS = False

    # create an empty dictionary of put spread candidates
    candidates = []

    for i, short_row in put_tbl_pd.iterrows():
        for j, long_row in put_tbl_pd.iterrows():

            my_long_strike = long_row['STRIKE']
            my_long_strike_int = int(float(my_long_strike))
            # print(f'put my_long_strike_int:{type(my_long_strike_int)}, value:{my_long_strike_int}')

            my_short_strike = short_row['STRIKE']
            my_short_strike_int = int(float(my_short_strike))
            # print(f'put my_short_strike_int:{type(my_short_strike_int)}, value:{my_short_strike_int}')


            # Calculate the criteria
            # The put strike price of the short leg must be greater than the strike price of the long leg


            
            if my_short_strike_int <= my_long_strike_int:
                if DEBUG_CANDIDATE_ANALYIS == True:
                    print()
                    print(f'skipping put pair, short strike {short_row['STRIKE']} is <= long strike {long_row['STRIKE']}')

                continue 


            if DEBUG_CANDIDATE_ANALYIS == True:
                print()
                print(f'adding put pair, short strike {short_row['STRIKE']} is > long strike {long_row['STRIKE']}')


            # strike_diff = abs(long_row['STRIKE'] - short_row['STRIKE'])
            strike_diff = abs(my_long_strike_int - my_short_strike_int)

            my_short_bid_fl = float(short_row['bid'])
            my_long_bid_fl = float(long_row['bid'])
            my_short_ask_fl = float(short_row['ask'])
            my_long_ask_fl = float(long_row['ask'])

            net_credit = my_short_bid_fl - my_long_bid_fl

            distance_to_spx = float(spx_last_fl - my_short_strike_int)

            my_short = short_row['STRIKE']
            my_long = long_row['STRIKE']

            if DEBUG_CANDIDATE_ANALYIS == True:
                print(f'my_short:{my_short}, type:{type(my_short)}')
                print(f'my_long:{my_long}, type:{type(my_long)}')
                print(f'strike_diff:{strike_diff}, type:{type(strike_diff)}')
                print(f'net_credit:{net_credit}, type:{type(net_credit)}')
                print(f'Put distance_to_spx:{distance_to_spx} type:{type(distance_to_spx)}')


            # # Check the rules for vertical spread candidates
            # if distance_to_spx >= min_spx_distance:
            #     pass
            # else:
            #     print(f'rejecting because of spx distance long:{distance_to_spx}')
            #     pass


            if (min_width <= strike_diff <= max_width and
                distance_to_spx >= min_spx_distance and
                min_net_credit <= net_credit <= max_net_credit and
                # min_long_val <= long_row['ASK'] <= max_long_val):
                min_long_val <= my_long_ask_fl <= max_long_val):
                

                # Append candidate dictionary with additional fields
                candidates.append({
                    "Short Strike": short_row['STRIKE'],
                    "Long Strike": long_row['STRIKE'],
                    "Short Bid": short_row['bid'],
                    "Long Ask": long_row['ask'],
                    "Net Credit": net_credit,
                    "SPX Offset": distance_to_spx,    # Add SPX Offset
                    "Spread Width": strike_diff       # Add Spread Width
                })

    # Convert candidates to DataFrame for easy viewing
    put_candidates_df = pd.DataFrame(candidates)
    # print(f'\nSPP1 Candidates for put credit spread:\n{put_candidates_df}')

    put_selection_df = pd.DataFrame() # empty dataframe


    # now select the best candidate

    if put_candidates_df.empty:
        put_selection_df = pd.DataFrame() # empty dataframe

    else:
    
        # Calculate the absolute difference from 1.80 for Net Credit
        put_candidates_df['Net Credit Diff'] = abs(put_candidates_df['Net Credit'] - optimal_net_credit)

        # Sort by Net Credit closest to 1.80 (primary) and Long Ask lowest (secondary)
        sorted_df = put_candidates_df.sort_values(by=['Net Credit Diff', 'Long Ask'])

        # Select the top row as the best candidate
        put_selection_df = sorted_df.head(1).drop(columns=['Net Credit Diff'])

        # # Print the result
        # print(f'\nput_selection_df:\n{put_selection_df}')

    return put_candidates_df, put_selection_df



def get_strike_int_from_sym(sym):

    # Find the index of "C0" or "P0"
    index = sym.find("C0")
    if index == -1:
        index = sym.find("P0")

    # Extract the next 4 characters
    if index != -1:
        strike_str = sym[index + 2:index + 6]
    else:
        print(f'Did not find C0 or P0 in sym:{sym}')
        return None

    strike_int = None
    
    try:
        strike_int = int(strike_str)
        # print(f'good integer string, strike_str:{strike_str}, strike_int:{strike_int}')
    except:
        print(f'bad integer string:{strike_str} from sym:{sym}')

    return strike_int



def calls_picker_optimal_net(call_tbl_pd, spx_last_fl):

    # print(f'527 in calls_picker_optimal_net, call_pd:\n{call_tbl_pd}')

    call_selection_df = pd.DataFrame() # empty dataframe
    call_candidates_df = pd.DataFrame() # empty dataframe



    DEBUG_CANDIDATE_ANALYIS = False

    # Reset dictionary of call spread candidates
    candidates = []

    for i, short_row in call_tbl_pd.iterrows():
        for j, long_row in call_tbl_pd.iterrows():

            # print(f'394 calls\nlong_row:\n{long_row}\nshort_row:\n{short_row}')

            my_long_strike = long_row['STRIKE']
            my_long_strike_int = int(float(my_long_strike))
            # print(f'call my_long_strike_int:{type(my_long_strike_int)}, value:{my_long_strike_int}')

            my_short_strike = short_row['STRIKE']
            my_short_strike_int = int(float(my_short_strike))
            # print(f'call my_short_strike_int:{type(my_short_strike_int)}, value:{my_short_strike_int}')

            
            # The call strike price of the short leg must be less than the strike price of the long leg
            if my_short_strike_int >= my_long_strike_int:
                if DEBUG_CANDIDATE_ANALYIS == True:
                    print()
                    print(f'skipping call pair, short strike {short_row['STRIKE']} is >= long strike {long_row['STRIKE']}')
                
                continue 

            if DEBUG_CANDIDATE_ANALYIS == True:
                print()
                print(f'adding call pair, short strike {short_row['STRIKE']} is < long strike {long_row['STRIKE']}')



            # strike_diff =  long_row['STRIKE'] - short_row['STRIKE']
            strike_diff = my_long_strike_int - my_short_strike_int


            my_short_ask_fl = float(short_row['ask'])
            my_long_ask_fl = float(long_row['ask'])
            my_short_bid_fl = float(short_row['bid'])
            my_long_bid_fl = float(long_row['bid'])


            net_credit = my_short_bid_fl - my_long_bid_fl
            
            distance_to_spx = float(my_short_strike_int - spx_last_fl)

            my_short = short_row['STRIKE']
            my_long = long_row['STRIKE']

            if DEBUG_CANDIDATE_ANALYIS == True:
                print(f'my_short:{my_short}, type:{type(my_short)}')
                print(f'my_long:{my_long}, type:{type(my_long)}')
                print(f'strike_diff:{strike_diff}, type:{type(strike_diff)}')
                print(f'net_credit:{net_credit}, type:{type(net_credit)}')
                print(f'Call distance_to_spx:{distance_to_spx} type:{type(distance_to_spx)}')


            # Check the rules for vertical spread candidates
            if (min_width <= strike_diff <= max_width and
                distance_to_spx >= min_spx_distance and
                min_net_credit <= net_credit <= max_net_credit and
                # min_long_val <= long_row['ASK'] <= max_long_val):
                min_long_val <= my_long_ask_fl <= max_long_val):

                # Append candidate dictionary with additional fields
                candidates.append({
                    "Short Strike": short_row['STRIKE'],
                    "Long Strike": long_row['STRIKE'],
                    "Short Bid": short_row['bid'],
                    "Long Ask": long_row['ask'],
                    "Net Credit": net_credit,
                    "SPX Offset": distance_to_spx,    # Add SPX Offset
                    "Spread Width": strike_diff       # Add Spread Width
                })

    # Convert candidates to DataFrame for easy viewing
    call_candidates_df = pd.DataFrame(candidates)
    # print(f'\nSPC1 Candidates for call credit spread:\n{call_candidates_df}')

    call_selection_df = pd.DataFrame() # empty dataframe
    
    
    # now select the best candidate

    if call_candidates_df.empty:
        call_selection_df = pd.DataFrame() # empty dataframe

    else:

        # Calculate the absolute difference from 1.80 for Net Credit
        call_candidates_df['Net Credit Diff'] = abs(call_candidates_df['Net Credit'] - optimal_net_credit)

        # Sort by Net Credit closest to 1.80 (primary) and Long Ask lowest (secondary)
        sorted_df = call_candidates_df.sort_values(by=['Net Credit Diff', 'Long Ask'])

        # Select the top row as the best candidate
        call_selection_df = sorted_df.head(1).drop(columns=['Net Credit Diff'])

        # Print the result
        # print(f'\ncall_selection_df:\n{call_selection_df}')

    return call_candidates_df, call_selection_df




def recommend_vertical_call_spread(call_tbl_pd, spx_last_fl):

    print(f'vertical_call_spread: spx_last_fl type: {type(spx_last_fl)}, value: {spx_last_fl}')

    # Step 1: Identify the short leg (bid closest to 1.90)
    short_leg = call_tbl_pd.iloc[(call_tbl_pd['bid'] - 1.90).abs().idxmin()]
    print(f'short_leg type: {type(short_leg)}, value:\n{short_leg}')

    # Ensure the short leg's STRIKE is below the SPX price (out of the money)
    if short_leg['STRIKE'] <= spx_last_fl:
        print(f"Short leg STRIKE ({short_leg['STRIKE']}) is in the money. Selecting next higher bid option.")
        # Select the next valid option with a STRIKE above SPX
        # short_candidates = call_tbl_pd.loc[(call_tbl_pd['bid'] - 1.90).abs().sort_values().index]
        short_candidates = call_tbl_pd.loc[(call_tbl_pd['bid'] - 1.80).abs().sort_values().index]
        short_leg = next(
            (row for _, row in short_candidates.iterrows() if row['STRIKE'] > spx_last_fl), 
            None
        )
        if short_leg is None:
            print("No suitable short leg found out of the money.")
            return

    # Step 2: Identify potential long legs (ask price is 0.10 and STRIKE > SPX)
    potential_long_legs = call_tbl_pd[(call_tbl_pd['ask'] == 0.10) & (call_tbl_pd['STRIKE'] > spx_last_fl)]

    # Step 3: Choose the long leg with the lowest STRIKE initially
    if potential_long_legs.empty:
        print("No options found with ask price of 0.10 and out of the money.")
        return
    long_leg = potential_long_legs.loc[potential_long_legs['STRIKE'].idxmin()]

    # Step 4: Ensure the STRIKE difference is less than or equal to 50
    while abs(short_leg['STRIKE'] - long_leg['STRIKE']) > 50:
        # Find the next long leg with a higher STRIKE
        potential_long_legs = potential_long_legs[potential_long_legs['STRIKE'] > long_leg['STRIKE']]
        if potential_long_legs.empty:
            print("No suitable long leg found within a 50-strike range.")
            return
        long_leg = potential_long_legs.loc[potential_long_legs['STRIKE'].idxmin()]

    # Print the recommended short and long legs
    print("Recommended Call Vertical Spread:")
    print(f"Short Leg: symbol={short_leg['symbol']}, STRIKE={short_leg['STRIKE']}, bid={short_leg['bid']}")
    print(f"Long Leg: symbol={long_leg['symbol']}, STRIKE={long_leg['STRIKE']}, ask={long_leg['ask']}")




def recommend_vertical_put_spread(put_tbl_pd, spx_last_fl):


    put_selection_df = pd.DataFrame() # empty dataframe
    put_candidates_df = pd.DataFrame() # empty dataframe

    # # Filter options where bid is closest to 1.90 for the short leg
    # short_leg = put_tbl_pd.iloc[(put_tbl_pd['bid'] - 1.90).abs().idxmin()]





    # Filter options where STRIKE is less than the provided spx_last_fl
    short_leg_candidates = put_tbl_pd[put_tbl_pd['STRIKE'] < spx_last_fl]

    # Ensure there are candidates left after filtering
    if not short_leg_candidates.empty:
        # Select the option whose bid is closest to 1.90
        short_leg = short_leg_candidates.iloc[(short_leg_candidates['bid'] - 1.90).abs().idxmin()]
    else:
        print("No options found with STRIKE less than SPX last price.")
        return











    print(f'405 puts short_leg type{type(short_leg)}, date:{short_leg}')








    # # Filter options with an ask price of 0.10 and get the one with the highest STRIKE
    # long_leg_candidates = put_tbl_pd[put_tbl_pd['ask'] == 0.10]
    # if not long_leg_candidates.empty:
    #     long_leg = long_leg_candidates.loc[long_leg_candidates['STRIKE'].idxmax()]
    # else:
    #     print("No options found with ask price of 0.10.")
    #     return
    


    # Filter options with an ask price greater than or equal to 0.10 and get the one with the lowest ask price
    long_leg_candidates = put_tbl_pd[put_tbl_pd['ask'] >= 0.10]
    if not long_leg_candidates.empty:
        long_leg = long_leg_candidates.loc[long_leg_candidates['ask'].idxmin()]
    else:
        print("No options found with ask price >= 0.10.")
        return




    # Adjust long leg if the strike difference is greater than 50
    while abs(short_leg['STRIKE'] - long_leg['STRIKE']) > 50:
        next_long_leg_candidate = put_tbl_pd[put_tbl_pd['STRIKE'] > long_leg['STRIKE']].sort_values('STRIKE').head(1)
        if next_long_leg_candidate.empty:
            print("No valid vertical spread found within the strike difference limit.")
            return 
        long_leg = next_long_leg_candidate.iloc[0]

    # Display the recommended short and long legs
    print("Recommended Put Vertical Spread:")
    print("Recommended Put Short Leg:")
    print(short_leg)
    print("\nRecommended Put Long Leg:")
    print(long_leg)

def puts_picker_short200_long010(put_tbl_pd, spx_last_fl):

    DEBUG_CANDIDATE_ANALYIS = True

    put_selection_df = pd.DataFrame() # empty dataframe
    put_candidates_df = pd.DataFrame() # empty dataframe

    print(f'spx:{spx_last_fl}, puts_picker_short200_long010 type:{type(put_tbl_pd)}, data:\n{put_tbl_pd}')


    recommend_vertical_put_spread(put_tbl_pd, spx_last_fl)


    return put_candidates_df, put_selection_df

 
    # Ensure ASK, BID, and STRIKE columns are of correct types
    put_tbl_pd['ASK'] = put_tbl_pd['ASK'].astype(float)
    put_tbl_pd['BID'] = put_tbl_pd['BID'].astype(float)
    put_tbl_pd['STRIKE'] = put_tbl_pd['STRIKE'].astype(int)

    # Step 1: Identify `long_row` (row with `ASK` closest to 0.10, breaking ties with highest `STRIKE`)
    target_ask = 0.10
    put_tbl_pd['ask_diff'] = abs(put_tbl_pd['ASK'] - target_ask)
    min_ask_diff = put_tbl_pd['ask_diff'].min()
    long_row = put_tbl_pd[put_tbl_pd['ask_diff'] == min_ask_diff].nlargest(1, 'STRIKE')
    print(f'200-010 put step 1 long_row:\n{long_row}')

    # Step 2: Identify `short_row` (row with `BID` closest to 2.00, breaking ties with lowest `STRIKE`)
    target_bid = 2.00
    put_tbl_pd['bid_diff'] = abs(put_tbl_pd['BID'] - target_bid)
    min_bid_diff = put_tbl_pd['bid_diff'].min()
    short_row = put_tbl_pd[put_tbl_pd['bid_diff'] == min_bid_diff].nsmallest(1, 'STRIKE')
    print(f'200-010 put step 2 short_row:\n{short_row}')

    # Step 2B: Reassign `short_row` if the absolute difference between `spx_last_fl` and `short_row['STRIKE']` is less than 10
    while abs(spx_last_fl - short_row['STRIKE'].values[0]) < 10:
        next_lowest_strike_rows = put_tbl_pd[put_tbl_pd['STRIKE'] < short_row['STRIKE'].values[0]]
        if next_lowest_strike_rows.empty:
            print(f'200-010 puts: cannot find short that is OTM by at least 10, aborting')
            return put_candidates_df, put_selection_df
        
        short_row = next_lowest_strike_rows.nlargest(1, 'STRIKE')

    print(f'200-010 put step 2B short_row:\n{short_row}')
    
    

    # Step 3: Adjust `long_row` if STRIKE difference is greater than 50
    while abs(short_row['STRIKE'].values[0] - long_row['STRIKE'].values[0]) > 50:
        next_highest_strike_rows = put_tbl_pd[put_tbl_pd['STRIKE'] > long_row['STRIKE'].values[0]]
        if next_highest_strike_rows.empty:
            print(f'200-010 puts: cannot find width that is <= 50, aborting')
            return put_candidates_df, put_selection_df
        
        long_row = next_highest_strike_rows.nsmallest(1, 'STRIKE')

    print(f'200-010 put step 3 long_row:\n{long_row}')

    # Step 5: Print the final `short_row` and `long_row`
    print(f'200-010 Short Row type:{type(short_row)}, data:\n{short_row}')
    # print(short_row[['STRIKE', 'BID', 'ASK']])
    # print()

    print(f'200-010 Long Row type:{type(long_row)}, data:\n{long_row}')
    # print(long_row[['STRIKE', 'BID', 'ASK']])
    # print()

    short_strike_int = int(short_row['STRIKE'].iloc[0])
    long_strike_int = int(long_row['STRIKE'].iloc[0])

    width = abs(short_strike_int - long_strike_int)
    print(f'200-010 puts width type:{type(width)}, value:{width}')

    candidates = []

    strike_diff = abs(short_strike_int - long_strike_int)

    # my_short_ask_fl = float(short_row['ASK'])
    my_short_ask_fl = float(short_row['ASK'].iloc[0])

   # my_long_ask_fl = float(long_row['ASK'])
    my_long_ask_fl = float(long_row['ASK'].iloc[0])

    # my_short_bid_fl = float(short_row['BID'])
    my_short_bid_fl = float(short_row['BID'].iloc[0])

    # my_long_bid_fl = float(long_row['BID'])
    my_long_bid_fl = float(long_row['BID'].iloc[0])

    net_credit = my_short_bid_fl - my_long_bid_fl
    distance_to_spx = float(spx_last_fl - short_row['STRIKE'].values[0])



    candidates.append({
                    # "Short Strike": short_row['STRIKE'],
                    "Short Strike": short_strike_int,
                    # "Long Strike": long_row['STRIKE'],
                    "Long Strike": long_strike_int,
                    # "Short Bid": short_row['BID'],
                    "Short Bid": my_short_bid_fl,
                    # "Long Ask": long_row['ASK'],
                    "Long Ask": my_long_ask_fl,
                    "Net Credit": net_credit,
                    "SPX Offset": distance_to_spx,    # Add SPX Offset
                    "Spread Width": strike_diff       # Add Spread Width
                })
    
    put_candidates_df = pd.DataFrame(candidates)
    put_selection_df = put_candidates_df 
    
    if DEBUG_CANDIDATE_ANALYIS == True:
        print(f'200-010 put_selection:\n{put_selection_df}')


    return put_candidates_df, put_selection_df


def calls_picker_short200_long010(call_tbl_pd, spx_last_fl):

    DEBUG_CANDIDATE_ANALYIS = True

    call_selection_df = pd.DataFrame() # empty dataframe
    call_candidates_df = pd.DataFrame() # empty dataframe

    print(f'spx:{spx_last_fl}, calls_picker_short200_long010 type:{type(call_tbl_pd)}, data:\n{call_tbl_pd}')

    recommend_vertical_call_spread(call_tbl_pd, spx_last_fl)

    return call_candidates_df, call_selection_df

 
    # Ensure ASK, BID, and STRIKE columns are of correct types
    call_tbl_pd['ASK'] = call_tbl_pd['ASK'].astype(float)
    call_tbl_pd['BID'] = call_tbl_pd['BID'].astype(float)
    call_tbl_pd['STRIKE'] = call_tbl_pd['STRIKE'].astype(int)

    # Step 1: Identify `long_row` (row with `ASK` closest to 0.10, breaking ties with highest `STRIKE`)
    target_ask = 0.10
    call_tbl_pd['ask_diff'] = abs(call_tbl_pd['ASK'] - target_ask)
    min_ask_diff = call_tbl_pd['ask_diff'].min()
    long_row = call_tbl_pd[call_tbl_pd['ask_diff'] == min_ask_diff].nlargest(1, 'STRIKE')
    print(f'200-010 call step 1 long_row:\n{long_row}')

    # Step 2: Identify `short_row` (row with `BID` closest to 2.00, breaking ties with lowest `STRIKE`)
    target_bid = 2.00
    call_tbl_pd['bid_diff'] = abs(call_tbl_pd['BID'] - target_bid)
    min_bid_diff = call_tbl_pd['bid_diff'].min()
    short_row = call_tbl_pd[call_tbl_pd['bid_diff'] == min_bid_diff].nsmallest(1, 'STRIKE')
    print(f'200-010 call step 2 short_row:\n{short_row}')

    # Step 2B: Reassign `short_row` if the absolute difference between `spx_last_fl` and `short_row['STRIKE']` is less than 10
    while abs(spx_last_fl - short_row['STRIKE'].values[0]) < 10:
        next_lowest_strike_rows = call_tbl_pd[call_tbl_pd['STRIKE'] > short_row['STRIKE'].values[0]]
        if next_lowest_strike_rows.empty:
            print(f'200-010 call: cannot find short that is OTM by at least 10, aborting')
            return call_candidates_df, call_selection_df
        
        short_row = next_lowest_strike_rows.nlargest(1, 'STRIKE')

    print(f'200-010 call step 2B short_row:\n{short_row}')
    
    

    # Step 3: Adjust `long_row` if STRIKE difference is greater than 50
    while abs(short_row['STRIKE'].values[0] - long_row['STRIKE'].values[0]) > 50:
        next_highest_strike_rows = call_tbl_pd[call_tbl_pd['STRIKE'] < long_row['STRIKE'].values[0]]
        if next_highest_strike_rows.empty:
            print(f'200-010 puts: cannot find width that is <= 50, aborting')
            return call_candidates_df, call_selection_df
        
        long_row = next_highest_strike_rows.nsmallest(1, 'STRIKE')

    print(f'200-010 call step 3 long_row:\n{long_row}')

    # Step 5: Print the final `short_row` and `long_row`
    print(f'200-010 calls Short Row type:{type(short_row)}, data:\n{short_row}')
    # print(short_row[['STRIKE', 'BID', 'ASK']])
    # print()

    print(f'200-010 calls Long Row type:{type(long_row)}, data:\n{long_row}')
    # print(long_row[['STRIKE', 'BID', 'ASK']])
    # print()

    short_strike_int = int(short_row['STRIKE'].iloc[0])
    long_strike_int = int(long_row['STRIKE'].iloc[0])

    width = abs(short_strike_int - long_strike_int)
    print(f'200-010 calls width type:{type(width)}, value:{width}')

    candidates = []

    strike_diff = abs(short_strike_int - long_strike_int)

    # my_short_ask_fl = float(short_row['ASK'])
    my_short_ask_fl = float(short_row['ASK'].iloc[0])

   # my_long_ask_fl = float(long_row['ASK'])
    my_long_ask_fl = float(long_row['ASK'].iloc[0])

    # my_short_bid_fl = float(short_row['BID'])
    my_short_bid_fl = float(short_row['BID'].iloc[0])

    # my_long_bid_fl = float(long_row['BID'])
    my_long_bid_fl = float(long_row['BID'].iloc[0])

    net_credit = my_short_bid_fl - my_long_bid_fl
    distance_to_spx = float(short_row['STRIKE'].values[0] - spx_last_fl)



    candidates.append({
                    # "Short Strike": short_row['STRIKE'],
                    "Short Strike": short_strike_int,
                    # "Long Strike": long_row['STRIKE'],
                    "Long Strike": long_strike_int,
                    # "Short Bid": short_row['BID'],
                    "Short Bid": my_short_bid_fl,
                    # "Long Ask": long_row['ASK'],
                    "Long Ask": my_long_ask_fl,
                    "Net Credit": net_credit,
                    "SPX Offset": distance_to_spx,    # Add SPX Offset
                    "Spread Width": strike_diff       # Add Spread Width
                })
    
    call_candidates_df = pd.DataFrame(candidates)
    call_selection_df = call_candidates_df 
    
    if DEBUG_CANDIDATE_ANALYIS == True:
        print(f'200-010 call_selection_df:\n{call_selection_df}')


    return call_candidates_df, call_selection_df


# puts_grid_candidates(): 
# Given a put option grid and the current SPX price, generates a list of all possible vertical call spread candidates that satisfy the rules
# put_tbl is a dataframe of put grid options that inclues Strike, Bid, and Ask columns
# spx_last is the last SPX price
# returns a dataframe with the list of put spread candidates
def puts_grid_candidates(put_tbl_pd, spx_last_fl):

    put_selection_df = pd.DataFrame() # empty dataframe
    put_candidates_df = pd.DataFrame() # empty dataframe

    if strategy == picker_config.STRATEGY_OPTIMAL_NET_CREDIT:
        put_candidates_df, put_selection_df = puts_picker_optimal_net(put_tbl_pd, spx_last_fl)

        return put_candidates_df, put_selection_df
    
    elif strategy == picker_config.STRATEGY_SHORT_200_LONG_010:
        pass
        put_candidates_df, put_selection_df = puts_picker_short200_long010(put_tbl_pd, spx_last_fl)

        return put_candidates_df, put_selection_df
    
    else:
        print(f'Unsupported picker strategy: {picker_config.PICKER_STRATEGY_DESC}')


    return put_candidates_df, put_selection_df


# calls_grid_candidates(): 
# Given a put option grid and the current SPX price, generates a list of all possible vertical call spread candidates that satisfy the rules
# call_tbl is a dataframe of put grid options that inclues Strike, Bid, and Ask columns
# spx_last is the last SPX price
# returns a dataframe with the list of put spread candidates
def calls_grid_candidates(call_tbl_pd, spx_last_fl):

    # print(f'936 in calls_grid_candidates, call_pd:\n{call_tbl_pd}')

    call_selection_df = pd.DataFrame() # empty dataframe
    call_candidates_df = pd.DataFrame() # empty dataframe

    if strategy == picker_config.STRATEGY_OPTIMAL_NET_CREDIT:
        call_candidates_df, call_selection_df = calls_picker_optimal_net(call_tbl_pd, spx_last_fl)

        return call_candidates_df, call_selection_df
    
    elif strategy == picker_config.STRATEGY_SHORT_200_LONG_010:
        call_candidates_df, call_selection_df = calls_picker_short200_long010(call_tbl_pd, spx_last_fl)

        return call_candidates_df, call_selection_df
    
    else:
        print(f'Unsupported picker strategy: {picker_config.PICKER_STRATEGY_DESC}')


    return call_candidates_df, call_selection_df



def calls_grid_candidates_list(call_tbl_list, spx_last_fl):

    call_pd = pd.DataFrame(call_tbl_list)

    # print(f'in calls_grid_candidates_list, call_pd:\n{call_pd}')

    call_candidates_df, call_selection_df = calls_grid_candidates(call_pd, spx_last_fl)

    # call_candidates_list = call_candidates_df.values.tolist()
    call_candidates_list = [call_candidates_df.columns.tolist()] + call_candidates_df.values.tolist()

    # call_selection_list = call_selection_df.values.tolist()
    call_selection_list = [call_selection_df.columns.tolist()] + call_selection_df.values.tolist()

    # print(f'995\ncall_candidates_list:\n{call_candidates_list}')
    # print(f'994 call_selection_df:\n{call_selection_df}')
    
    return call_candidates_list, call_selection_list


def puts_grid_candidates_list(put_tbl_list, spx_last_fl):

    put_pd = pd.DataFrame(put_tbl_list)

    # print(f'in puts_grid_candidates_list(), put_pd:\n{call_pd}')

    put_candidates_df, put_selection_df = puts_grid_candidates(put_pd, spx_last_fl)

    # put_candidates_list = put_candidates_df.values.tolist()
    put_candidates_list = [put_candidates_df.columns.tolist()] + put_candidates_df.values.tolist()

    # put_selection_list = put_selection_df.values.tolist()
    put_selection_list = [put_selection_df.columns.tolist()] + put_selection_df.values.tolist()

    # print(f'984 put_selection_df:\n{put_selection_df}')
    # print(f'985\nput_candidates_list:\n{put_candidates_list}')

    return put_candidates_list, put_selection_list






# Function to check if value is None or NaN
def is_valid(value):
    return value is not None and not math.isnan(value)   

def display_list(option_list):
    try:
        for option in option_list:
            print(f"Symbol: {option['symbol']}")
            print(f"  Bid: {option['bid']} (Time: {option['bid_time']})")
            print(f"  Ask: {option['ask']} (Time: {option['ask_time']})")
            print(f"  Last: {option['last']} (Time: {option['last_time']})")
            print(f"  Strike: {option['STRIKE']}")
            print("-" * 40)
    except KeyError as e:
        print(f"KeyError: Missing key {e}. Exiting function.")
        return
    except Exception as e:
        print(f"An error occurred: {e}. Exiting function.")
        return
    
  

def display_lists(opt_short, opt_long):

    # print(f'displaying both lists\n')

    # print(f'in display_lists opt_short\n  opt_short type:{type(opt_short)}, data:{opt_short}')
    # print(f'in display_lists opt_long\n  opt_long type:{type(opt_long)}, data:{opt_long}')
    # print(f'len of call_list:{len(call_list)}, len of put_list:{len(put_list)}')

    # print(f'call_list:\n{call_list}')


    print(f'short option:')

    try:
        item = opt_short[0]
        print(f"  Symbol: {item['symbol']}")
        print(f"  Bid: {item['bid']}")
        print(f"  Ask: {item['ask']}")
        print(f"  Last: {item['last']}")
        print(f"  Strike: {item['STRIKE']}")

    except Exception as e:
        print(f'An error occurred: {e} while displaying opt_short, copt_short data:\n{opt_short}')

    print(f'long option:')

    try:
        item = opt_long[0]
        print(f"Symbol: {item['symbol']}")
        print(f"  Bid: {item['bid']}")
        print(f"  Ask: {item['ask']}")
        print(f"  Last: {item['last']}")
        print(f"  Strike: {item['STRIKE']}")

    except Exception as e:
        print(f'An error occurred: {e} while displaying opt_long, opt_long data:\n{opt_long}')


def ten_max(option_list, spx_price, option_type, atm_straddle_value):

    DEBUG_TEN_MAX = False
        
    try:

        # print(f'in ten_max(), option_type:{option_type}, spx:{spx_price}')
        # print(f'in ten_max(), option_list type:{type(option_list)}, data:\n{option_list}')


        # base_short_target = 1.8
        base_short_target = max_short_target
        my_short_target = base_short_target
        atm_short_target = atm_straddle_value * 0.22

        if DEBUG_TEN_MAX == True:
            print(f'base_short_target:{base_short_target} atm_short_target:{atm_short_target}, min_short_target:{min_short_target}')

        if atm_short_target < my_short_target:
            if DEBUG_TEN_MAX == True:
                print(f'atm target is less than my short (base) target, my_target is now atm target')
            
            my_short_target = atm_short_target

        else:
            pass
            # print(f'atm target is not less than my short (base) target')


        if my_short_target < min_short_target:
            # print(f'my_short_target is less than min_short_target:{min_short_target}, my_short_target is now min_short_target')
            my_short_target = min_short_target
        else:
            # print(f'my_short_target is not less than min_short_target:{min_short_target}, keeping my_short')
            pass




        # print(f'base_short_target:{base_short_target:.2f}, atm_short_target:{atm_short_target:.2f}, my_short_target:{my_short_target:.2f}')



        # print(f'ten_max() atm_straddle_value type{type(atm_straddle_value)}, value:{atm_straddle_value}')
        # print(f'ten_max() max_short_target type{type(max_short_target)}, value:{max_short_target}')
        # print(f'ten_max() min_short_target type{type(min_short_target)}, value:{min_short_target}')
        # print(f'ten_max() my_short_target type: {type(my_short_target)}, value: {my_short_target:.2f}')

    
        # Filter the out-of-the-money (OTM) options based on the type (CALL or PUT)
        if option_type == "CALL":
            otm_list = [opt for opt in option_list if opt.get('STRIKE') and opt['STRIKE'] > (spx_price + 2)]
        elif option_type == "PUT":
            otm_list = [opt for opt in option_list if opt.get('STRIKE') and opt['STRIKE'] < (spx_price - 2)]
        else:
            print("Invalid option_type:<{option_type}>. Must be 'CALL' or 'PUT'.")
            return [], []


        # print(f'20 otm_list len:{len(otm_list)}, data:')
        # display_list(otm_list)


        if not otm_list:
            print("No OTM options found.")
            return [], []
        

        if DEBUG_TEN_MAX == True:
            print(f'my_short_target used:{my_short_target}')
        

        


        # # Select the short_leg based on the bid closest to 2.00
        # short_leg = min(otm_list, key=lambda opt: abs(opt.get('bid', float('inf')) - 2.00))
        # Select the short_leg based on the bid closest to my_short_target
        short_leg = min(otm_list, key=lambda opt: abs(opt.get('bid', float('inf')) - my_short_target))
        # print(f'Short Leg Selected.  short_leg type:{type(short_leg)},\n  data:<{short_leg}>')

        if not short_leg:
            print("No valid short_leg found")
            return [], []
        

        



        # Filter for the fifty_max_list: further OTM and 20 <= strike difference <= 50
        fifty_max_list = [
            opt for opt in option_list
            if opt.get('STRIKE') and 
            (opt['STRIKE'] > short_leg['STRIKE'] if option_type == "CALL" else opt['STRIKE'] < short_leg['STRIKE']) and 
            # 20 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50
            15 <= abs(opt['STRIKE'] - short_leg['STRIKE']) <= 50

        ]
        
        # print(f'fifty_max_list len:{len(fifty_max_list)}, data:')
        # display_list(fifty_max_list)

        if not fifty_max_list:
            print("No valid fifty_max options found.")
            return [], []


        # Select the long_leg based on the lowest 'bid' but at least 0.05
        # print(f'Selecting long leg from fifty_max_list type:{type(fifty_max_list)}, data:\n{fifty_max_list}')
        # long_leg = [opt for opt in fifty_max_list if opt.get('bid', 0) >= 0.05]

        # Filter the list to include items with 'bid' >= 0.05
        valid_bids = [item for item in fifty_max_list if item['bid'] >= 0.05]

        # Select the dictionary with the lowest 'bid' value, 
        #   breaking ties for PUTs by the highest 'STRIKE'
        #   breaking ties for CALLs by the lowest 'STRIKE'
        if valid_bids:
            if option_type == "PUT":
                long_leg_dict = min(valid_bids, key=lambda x: (x['bid'], -x['STRIKE']))
            else:
                long_leg_dict = min(valid_bids, key=lambda x: (x['bid'], +x['STRIKE']))

            long_leg = [long_leg_dict]  # Wrap the selected dictionary in a single-item list
        else:
            long_leg = []  # If no valid bids are found, return an empty list

        # print(f'Long Leg Selected.  long_leg type:{type(long_leg)},\n  data:<{long_leg}>')

        if not long_leg:
            # print("No valid long_leg found with bid >= 0.05")
            return [short_leg], []
        
        # long_leg is a list.  Convert short_leg to a list
        short_leg = [short_leg]


        # short_leg and long_leg are both type list, each with a single dictionary
        return short_leg, long_leg


    except KeyError as e:
        print(f"Missing key: {e}. Ensure all options have 'symbol', 'bid', 'ask', and 'STRIKE' keys.")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return [], []  # Return empty lists on any exception or rule violation





def calculate_atm_straddle_value(chain):

    DEBUG_ATM_VALUE = False

    try:

        # Extract the SPX last price
        spx_last = None
        for item in chain:
            if item['symbol'] == '$SPX':
                spx_last = item['last']
                break

        if spx_last is None:
            print("atm_straddle: SPX last price not found in the chain")
            return None

        # Initialize variables to hold the closest Call and Put options
        closest_call = None
        closest_put = None
        closest_strike_diff = float('inf')

        # Iterate over the chain to find the closest ATM Call option
        for item in chain:
            if 'C0' in item['symbol']:
                strike_price = int(item['symbol'].split('C0')[1][:4])
                strike_diff = abs(spx_last - strike_price)
                if strike_diff < closest_strike_diff:
                    closest_strike_diff = strike_diff
                    closest_call = item

        # Find the corresponding Put option with the same strike price
        for item in chain:
            if 'P0' in item['symbol']:
                strike_price = int(item['symbol'].split('P0')[1][:4])
                if closest_call and strike_price == int(closest_call['symbol'].split('C0')[1][:4]):
                    closest_put = item
                    break

        if closest_call is None or closest_put is None:
            print("atm_straddle: Matching Call or Put option not found in the chain")
            return None

        # Calculate the value of the ATM straddle
        straddle_value = closest_call['bid'] + closest_put['bid']


        if DEBUG_ATM_VALUE == True:

            # Display the results
            print(f"Call Option Symbol: {closest_call['symbol']}")
            print(f"Put Option Symbol: {closest_put['symbol']}")
            print(f"Call Bid Value: {closest_call['bid']}")
            print(f"Put Bid Value: {closest_put['bid']}")
            print(f"ATM Straddle Value: {straddle_value:.2f}")

        return straddle_value

    except Exception as e:
        print(f"atm_straddle: An error occurred: {e}")
        return None







# returns four type:<class 'list'> variables
#[['Short Strike', 'Long Strike', 'Short Bid', 'Long Ask', 'Net Credit', 'SPX Offset', 'Spread Width'], [6030.0, 5980.0, 1.6, 0.1, 1.5, 10.1899999999996, 50.0]]
    

def generate_recommendation(grid):

    my_call_short = []
    my_call_long = []
    my_put_short = []
    my_put_long = []
    spx_last_fl = None

    
    # print(f'grid type:{type(grid)}, data:\n{grid}')

    atm_straddle_value = calculate_atm_straddle_value(grid)
    if atm_straddle_value == None:
        print(f'unable to calculate the ATM straddle')
        return my_call_short, my_call_long, my_put_short, my_put_long, spx_last_fl
    
    if atm_straddle_value < 1:
        print(f'ATM straddle value too low: {atm_straddle_value}')
        return my_call_short, my_call_long, my_put_short, my_put_long, spx_last_fl
    


    # pretty_grid = json.dumps(grid, indent=4)
    # print(f'pretty_grid:\n{pretty_grid}')

    # Initialize empty lists
    put_list = []
    call_list = []
    call_candidates = []
    call_recommendation = []
    put_candidates = []
    put_recommendation = []

    

    # Iterate through each item in the grid list
    for item in grid:

        # print(f'385 processing item :\n{item}')

        if 'symbol' in item:

            # print(f'processing item with a symbol:\n{item}')

            my_sym = item['symbol']

            # print(f"489 symbol: {my_sym}")

            if my_sym == "$SPX":

                my_last = None

                # print(f"my_sym is $SPX")

                if 'last' in item:
                    spx_last_fl = float(item['last'])

            else:
                # print(f"my_sym is NOT $SPX")
                pass


            if 'bid' not in item:
                # print(f"bid not in item")
                continue
            else:
                # print(f"Bid: {item['bid']}")
                pass

            if 'ask' not in item:
                # print(f"ask not in item")
                continue
            else:
                # print(f"Ask: {item['ask']}")
                pass

            if 'last' not in item:
                # print(f"last not in item")
                continue
            else:
                # print(f"Last: {item['last']}")
                pass

            


            # Add item to either put_lis or call_list
            

            if 'P0' in item['symbol'] and is_valid(item['bid']) and is_valid(item['ask']):
                # get the strike price from the symbol
                strike_int = get_strike_int_from_sym(item['symbol'])
                item['STRIKE'] = strike_int

                # print(f'adding item <{item}> to put_list')
                put_list.append(item)
            
            # Check for call options and valid bid/ask
            if 'C0' in item['symbol'] and is_valid(item['bid']) and is_valid(item['ask']):
                # get the strike price from the symbol
                strike_int = get_strike_int_from_sym(item['symbol'])
                item['STRIKE'] = strike_int

                # print(f'adding item <{item}> to call_list')
                call_list.append(item)



 

    if spx_last_fl != None:

        # print(f'\n10 call_list type:{type(call_list)}, data:\n{call_list}\n')
        # print(f'\n20 put_list type:{type(put_list)}, data:\n{put_list}\n')


        my_call_short, my_call_long = ten_max(call_list, spx_last_fl, "CALL", atm_straddle_value)
        # print(f'\n10 ten_max CALL returned\nshort:{my_call_short}\nlong:{my_call_long}\n')


        # print(f'\n20 ten_max CALL returned:')
        # print(f'Call Spread:')
        # display_lists(my_call_short, my_call_long)

        my_put_short, my_put_long = ten_max(put_list, spx_last_fl, "PUT", atm_straddle_value)
        # print(f'\n30 ten_max PUT returned\nshort:{my_put_short}\nlong:{my_put_long}\n')
        
        # print(f'\n40 ten_max PUT returned:')
        # print(f'Put Spread:')
        # display_lists(my_put_short, my_put_long)


        # call_candidates, call_recommendation = calls_grid_candidates_list(call_list, spx_last_fl)
        # print(f'094C call_candidates:\n{call_candidates}\ncall_recommendation type:{type(call_recommendation)}, data:\n{call_recommendation}')

        # put_candidates, put_recommendation = puts_grid_candidates_list(put_list, spx_last_fl)
        # print(f'094P put_candidates:\n{put_candidates}\nput_recommendation type:{type(put_recommendation)}, data:\n{put_recommendation}')

        # call_recommendation_len = len(call_recommendation)
        # put_recommendation_len = len(put_recommendation)

        # print(f'call len:{call_recommendation_len}, put_len:{put_recommendation_len}')


        # print(f'294C call_recommendation:\n{call_recommendation}')
        # pretty_json = json.dumps(call_candidates, indent=4)
        # print(f'794C call pretty_json:\n{pretty_json}')

    
    else:
        print(f'094 call spx_last_fl was None')
        pass


    return my_call_short, my_call_long, my_put_short, my_put_long, spx_last_fl



    # return call_candidates, call_recommendation, put_candidates, put_recommendation

    









    














