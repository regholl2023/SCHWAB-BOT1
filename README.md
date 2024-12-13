# schwab-streamer
schwab-streamer (the streamer): a python project for Schwab Developer API streaming quotes for SPX and SPX 0DTE options

# How To
Install python and the python libraries listed at the beginning of the streamer.py file

Install an mqtt broker on your computer.  Mosquitto broker is recommended

For troubleshooting, the MQTT Explorer app is recommended

Create a schwab developer account and create an app

Uses the schwabdev library deveveloped by Tyler Bowers
https://github.com/tylerebowers/Schwab-API-Python
Follow the instructions in schwabdev README

Your project directory must contain a .env file with these contents <br>
MY_APP_KEY=<your_schwab_app_key> <br>
MY_SECRET_KEY=<your_schwab_secret_key> <br>
TOKENS_FILE_PATH=<tokens_file_pathspec>

example TOKENS_FILE_PATH: 
TOKENS_FILE_PATH='C:\Users\me\streamer_proj\tokens.json'

If the tokens.json file does not exist, the schwabdev library will create it.
schwabdev then maintains the tokens.json file as the access token and refresh token are refreshed.  
schwabdev automatically updates the access token when it expires.
When the refresh token has expired and streamer

schwab-streamer (the streamer) uses your schwab developer API tokens subscibes and subscribes to streaming 
SPX quotes and 0DTE SPX option quotes. The streamer monitors SPX open/high/low/close of the current day and 
expands that range another 20 strikes.  That expanded range is used for streaming quote subscriptions with Schwab API.  
As SPX reaches new daily highs or lows, that range of strike quotes automatically expands. 

As streaming quotes are received from Schwab, they are re-published through the localhost mqtt broker using this mqtt topic: <br>
___schwab/stream___ <br>
with the quote data as payload

# Individual apps
- streamer.py: Subscribes to streaming SPX and selected SPX option quotes through the schwab api.  Publishes real time via mqtt on localhost. Periodically queries the same SPX option prices to ensure that we have current and accurate prices.
- grid.py: Subscribes to mqtt (localhost) for SPX and SPX option data that is published by the streamdr.  Maintains current SPX and SPX option grid/chain. Publishs the grid on localhost mqtt upon request
- chain.py: Subscribes to mqtt (localhost) for SPX and SPX option data that is published by the streamdr.  Maintains current SPX and SPX option grid/chain.  Once a minute, persists the current SPX and SPX option price data to a .json file for future evaluation.
- gamma_track.py: Subscribes to all streamed SPX option data.  For each reported gamma value 0.05 or higher, persists the reported gamma data in a .pkl file for later evaluation.

# Other files the provide data or functions to be used by the aps
- recommender.py: Based on a snapshot of the current SPX option grid/chain data, recommends Call and Put vertical spreads to be used in MEIC or METF trades.
- picker_config.py: Provides parameters to be used the the recommender.
- VERSION: schwab-streamer version
