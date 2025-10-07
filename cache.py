from datetime import datetime, timedelta, time as dt_time
from collections import deque, defaultdict
from typing import List, Dict, Any, Tuple, Union
from datetime import timezone
import numpy as np
import pandas as pd
from models import Conv2DMultiBinary

from indicators import ( LOCAL_FUNCS, AGAUSSIAN, PERCENTILE,
    HUGE, CONSTANT, parse_param_to_inputs, load_indicator_input, load_indicator_output
)

from av_client import av_client, process_REALTIME_BULK_QUOTES

import torch
from urllib.parse import urlencode, parse_qs
import copy


import matplotlib.pyplot as plt
import seaborn as sns
import pytz

est_timezone = pytz.timezone('America/New_York')

class RealTimeInferenceEngine:
    def __init__(self, tickers, accuracy_model_path, profit_model_path, accuracy_basefilters, profit_basefilters, context_depth, params, hot_burn_in=250, device='cuda'):
        # --- This __init__ method remains the same ---
        print("For any technical issues please submit a ticket to https://github.com/aqjshi/Quantitative_AI_Public")
        self.tickers = tickers
        self.device = torch.device(device if torch.cuda.is_available() else 'cpu')
        print("Using device: ", device)
        self.hot_burn_in = hot_burn_in
        print("Loading dual models...")
        num_outputs = len(self.tickers)
        self.profit_model = Conv2DMultiBinary(in_channels=1, base_filters=profit_basefilters, num_outputs=num_outputs).to(self.device)
        self.profit_model.load_state_dict(torch.load(profit_model_path, map_location=self.device))
        self.profit_model.eval()
        print(f"✅ Profit Model loaded from {profit_model_path}")
        self.accuracy_model = Conv2DMultiBinary(in_channels=1, base_filters=accuracy_basefilters, num_outputs=num_outputs).to(self.device)
        self.accuracy_model.load_state_dict(torch.load(accuracy_model_path, map_location=self.device))
        self.accuracy_model.eval()
        print(f"✅ Accuracy Model loaded from {accuracy_model_path}")
        self.ohlcv_funcs = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
        self.context_depth = context_depth
        self.params = sorted(params)
        self.hot_raw_ohlcv_data_buffer = {}
        for ticker in self.tickers:
            self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=TIMESTAMP"] = deque(maxlen=hot_burn_in)
            for func in self.ohlcv_funcs:
                self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function={func}"] = deque(maxlen=hot_burn_in)
        self.param_to_idx = {param: i for i, param in enumerate(self.params)}
        print(f"Engine initialized for {len(self.tickers)} tickers.")


        # Iterate through each ticker
        for ticker in self.tickers:
            # Add the TIMESTAMP deque for the current ticker
            self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=TIMESTAMP"] = deque(maxlen=hot_burn_in)

            # Then iterate through each OHLCV function and add its deque
            for func in self.ohlcv_funcs:
                self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function={func}"] = deque(maxlen=hot_burn_in)

        
        self.hot_params_tensor = {
            ticker: np.zeros((self.context_depth, len(self.params)), dtype=np.float32)
            for ticker in self.tickers

        }
        self.hot_ohlcv_realtime_handler = {
            f"ticker={ticker}&function={func}": 0
            for ticker in self.tickers for func in self.ohlcv_funcs
        }

        self.param_to_idx = {param: i for i, param in enumerate(self.params)}
        
        print(f"Engine initialized for {len(self.tickers)} tickers.")
        self.first_update = {
            f"{ticker}": True
            for ticker in self.tickers
        }
        
        

    def cold_start(self):
        print("--- Starting Cold Start via API ---")
        av = av_client()
        raw_ohlcv_data = {}
        current_date = datetime.now()
        current_month_yyyy_mm = current_date.strftime("%Y-%m")


        for ticker in self.tickers:
            print(f"Fetching historical data for {ticker}...")
            try:
                series_data = av.fetch_intraday(
                    symbol=ticker, 
                    month=current_month_yyyy_mm,
                    interval="1min",
                    outputsize="full", # Ensure you get enough historical data
                    extended_hours=True,
                    entitlement="realtime"
                )

                if not series_data:
                    print(f"⚠️ Warning: No data returned from API for {ticker}.")
                    continue
                
                # 1. Create DataFrame directly from the fetched dictionary
                df = pd.DataFrame.from_dict(series_data, orient='index')
                df.index = pd.to_datetime(df.index, format='%Y-%m-%d %H:%M:%S') 
                
                # Localize the naive index to 'America/New_York' (EST/EDT)
                df.index = df.index.tz_localize(est_timezone, ambiguous='infer') # 'infer' handles DST
                
                # 2. Rename columns
                df = df.rename(columns={
                    '1. open': 'OPEN', '2. high': 'HIGH', 
                    '3. low': 'LOW', '4. close': 'CLOSE', '5. volume': 'VOLUME'
                })
                
                # 3. Convert index to datetime and sort (Alpha Vantage gives newest first)
                df.index = pd.to_datetime(df.index)
                df = df.sort_index() # Sort to have oldest first (ascending time)

                # 4. Convert OHLCV data to numeric types
                # Use .apply(pd.to_numeric) for robustness, handling errors
                for col in self.ohlcv_funcs:
                    df[col] = pd.to_numeric(df[col], errors='coerce') # 'coerce' will turn non-numeric to NaN
                
                # Set volume to integer, handling NaNs from conversion if any
                df['VOLUME'] = df['VOLUME']




                # 3. Sort to have oldest first (ascending time)
                df = df.sort_index()

                # 4. Convert OHLCV data to numeric types
                for col in self.ohlcv_funcs:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # --- NEW GRANULAR FILTERING LOGIC STARTS HERE ---
                if not df.empty:
                    price_columns_to_check = ['OPEN', 'HIGH', 'LOW', 'CLOSE']
                    for col in price_columns_to_check:
                        # Calculate price change over the last 5 periods (minutes)
                        df['price_change'] = df[col].pct_change(periods=100).abs()

                        # Define conditions for anomalous data
                        volume_condition = df['VOLUME'] < 800000
                        price_fluctuation_condition = df['price_change'] > 0.01 # Using 5% threshold
                        anomalous_rows_mask = volume_condition & price_fluctuation_condition

                        # Count anomalies before correction
                        num_anomalies = anomalous_rows_mask.sum()
                        if num_anomalies > 0:
                            print(f"🔎 Found {num_anomalies} anomalous data points in '{col}' for {ticker} (5-min >5% change). Correcting them.")

                        # For the anomalous rows, set only the specific column's value to NaN
                        df.loc[anomalous_rows_mask, col] = np.nan

                        # Forward-fill only the specific column to preserve other data in the row
                        df[col] = df[col].ffill()

                        # Drop the temporary column before the next iteration
                        df = df.drop(columns=['price_change'])
                
                if not df.empty:
                    latest_data_time = df.index[-1] # Most recent time in the fetched data
                    print("latest_data_time ", latest_data_time)
                    
                    current_utc_minute_rounded = datetime.now(df.index.tz).replace(second=0, microsecond=0)
                    start_time_for_range = latest_data_time - timedelta(minutes=self.hot_burn_in - 1)
                    
                    full_time_index = pd.date_range(
                        start=start_time_for_range,
                        end=latest_data_time,
                        freq='1min',
                        name='timestamp', # Name the index for clarity
                        tz=df.index.tz # Ensure timezone consistency
                    )
                    print(f"length of linspace", len(full_time_index))
                    
                    reindexed_df = df.reindex(full_time_index)

                    
                    reindexed_df[['OPEN', 'HIGH', 'LOW', 'CLOSE']] = reindexed_df[['OPEN', 'HIGH', 'LOW', 'CLOSE']].ffill().bfill().fillna(0).astype(float)
                    
                    # Fill VOLUME with 0 for missing entries
                    reindexed_df['VOLUME'] = reindexed_df['VOLUME'].fillna(0).astype(float)
                    


                    reindexed_df[['OPEN', 'HIGH', 'LOW', 'CLOSE']] = reindexed_df[['OPEN', 'HIGH', 'LOW', 'CLOSE']]
       
       
                    self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=TIMESTAMP"] = reindexed_df.index[:self.hot_burn_in]

                    self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=OPEN"] = reindexed_df['OPEN'][:self.hot_burn_in]
                    self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=HIGH"] = reindexed_df['HIGH'][:self.hot_burn_in]
                    self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=LOW"] = reindexed_df['LOW'][:self.hot_burn_in]
                    self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=CLOSE"] = reindexed_df['CLOSE'][:self.hot_burn_in]
                    self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=VOLUME"] = reindexed_df['VOLUME'][:self.hot_burn_in]

                    self.hot_ohlcv_realtime_handler[f"ticker={ticker}&function=OPEN"] = self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=OPEN"].iloc[-1]
                    self.hot_ohlcv_realtime_handler[f"ticker={ticker}&function=HIGH"]  = self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=HIGH"].iloc[-1]
                    self.hot_ohlcv_realtime_handler[f"ticker={ticker}&function=LOW"]  = self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=LOW"].iloc [-1]
                    self.hot_ohlcv_realtime_handler[f"ticker={ticker}&function=CLOSE"]  = self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=CLOSE"].iloc[-1]
                    self.hot_ohlcv_realtime_handler[f"ticker={ticker}&function=VOLUME"] = self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=VOLUME"].iloc[-1]

                    # print(self.hot_ohlcv_realtime_handler[f"ticker={ticker}&function=VOLUME"] )

                    # print(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=TIMESTAMP"])
                    # print(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=OPEN"])
                    # print(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=HIGH"])
                    # print(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=LOW"])
                    # print(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=CLOSE"])
                    # print(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=VOLUME"])
                else:
                    print(f"⚠️ Warning: DataFrame for {ticker} is empty after initial fetch. Cannot reindex.")
                
            except Exception as e:
                print(f"❌ Failed to process data for {ticker}. Error: {e}")
        # print(self.hot_raw_ohlcv_data_buffer.keys())

        
        # print("Calculating mean for each series:")
        # for key in self.hot_raw_ohlcv_data_buffer.keys():
        #     if re.search(r"TIMESTAMP", key):
        #         continue
        #     else:
        #         data_series = self.hot_raw_ohlcv_data_buffer[key]
        #         mean_value = statistics.mean(data_series)
        #         print(f"  {key}: Mean = {mean_value:.4f}")

        print("✅ Cold start complete. Tensors and buffers are ready.")


    def _calculate_param_slice(self, param_str: str, local_hot_raw_ohlcv_data_buffer: Dict[str, deque]) -> pd.Series:
        """
        Reconstructs an OHLCV DataFrame for a ticker and calculates one indicator series.
        """
        # 1. Extract ticker from the param string
        ticker = param_str.split("ticker=")[1].split("&")[0]
        input_dict, key = parse_param_to_inputs(param_str)
        # --- DIAGNOSIS PRINTING STARTS HERE ---
        # print(f"\n--- Diagnosing _calculate_param_slice for {param_str} (Ticker: {ticker}) ---")
        # print(f"Keys in local_hot_raw_ohlcv_data_buffer for {ticker}:")
        
        # # Print all relevant keys for the current ticker and their deque lengths
        # re_pattern = f"ticker={ticker}" # This correctly forms the pattern string, e.g., "ticker=NVDA"
        # all_ticker_keys = [k for k in local_hot_raw_ohlcv_data_buffer.keys() if re.search(re_pattern, k)]

        # for k in all_ticker_keys:
        #     deque_len = len(local_hot_raw_ohlcv_data_buffer[k])
        #     print(f"  {k}: Length = {deque_len}, MaxLen = {local_hot_raw_ohlcv_data_buffer[k].maxlen}")
        #     # Print actual data for OHLCV to see NaNs or zeros
        #     if 'function=TIMESTAMP' in k or 'function=OPEN' in k or 'function=HIGH' in k or 'function=LOW' in k or 'function=CLOSE' in k or 'function=VOLUME' in k:
        #         print(f"    Data (first 5, last 5): {list(local_hot_raw_ohlcv_data_buffer[k])[:5]} ... {list(local_hot_raw_ohlcv_data_buffer[k])[-5:]}")
        #         if any(pd.isna(x) for x in local_hot_raw_ohlcv_data_buffer[k]):
        #             print(f"    *** WARNING: {k} contains NaN values! ***")
        #         if not local_hot_raw_ohlcv_data_buffer[k]:
        #             print(f"    *** WARNING: {k} is empty! ***")

        
        # --- DIAGNOSIS PRINTING ENDS HERE ---


        timestamp_key = f"ticker={ticker}&function=TIMESTAMP"
        timestamps_list = list(local_hot_raw_ohlcv_data_buffer.get(timestamp_key, deque()))
        
        # Check if timestamps are available and valid
        if not timestamps_list:
            print(f"Warning: No TIMESTAMP data found for {ticker} in buffer. Cannot create DataFrame.")
            return pd.DataFrame(dtype=np.float64) # Return an empty DataFrame




        data_for_df_columns = {}
        for func in self.ohlcv_funcs + ["TIMESTAMP"]: # e.g., "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"
            buffer_key = f"ticker={ticker}&function={func}"
            data_list = list(local_hot_raw_ohlcv_data_buffer.get(buffer_key, deque()))
            data_for_df_columns[func] = data_list 



        all_lengths = [len(timestamps_list)] + [len(lst) for lst in data_for_df_columns.values()]
        if not all_lengths or min(all_lengths) == 0:
            print(f"WARNING: Data for {ticker} is empty or has inconsistent lengths. Lengths: {all_lengths}")
            return pd.DataFrame(dtype=np.float64)
        
        
        min_len = min(all_lengths)

        timestamps_list = timestamps_list[-min_len:]
        for col_name in data_for_df_columns:
            data_for_df_columns[col_name] = data_for_df_columns[col_name][-min_len:]

        
        # Convert timestamps to datetime objects immediately
        df = pd.DataFrame(data_for_df_columns)
        # print(df.shape)
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], utc=True)
        df = df.set_index('TIMESTAMP')#.sort_index() # Set timestamp as index and sort
        # print(df.index)                
        

        fn = LOCAL_FUNCS[key[0]]
        
        # Pass the DataFrame and parsed inputs to the indicator function
        result_series = fn(df, input_dict, key)
        # print(result_series.mean())
        return     result_series
        #    

    def update_hot_tensor_and_return_inference_item(self, processing_tickers: List[str], new_quote_data: Dict[str, Dict], update: bool) -> np.ndarray:
        # new_timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        new_timestamp  = (datetime.now(timezone.utc) + timedelta(minutes=1)).replace(second=0, microsecond=0)

        # for ticker in processing_tickers:
        #     print(new_quote_data[f"{ticker}"])
        
        temp_buffer_for_next_state = {
            key: deque(list(dq), maxlen=self.hot_burn_in) # Create new deque with copied contents
            for key, dq in self.hot_raw_ohlcv_data_buffer.items()
        }
        
        popped_items_for_logging = []
        for ticker_symbol, data_for_ticker in new_quote_data.items(): 
            if ticker_symbol in self.tickers and update: # Ensure we only process tracked tickers
                popped_ts = temp_buffer_for_next_state[f"ticker={ticker_symbol}&function=TIMESTAMP"].popleft()
                
                popped_ohlcv_data = {'timestamp': popped_ts.isoformat()}
                for func_name in self.ohlcv_funcs:
                    func_key = f"ticker={ticker_symbol}&function={func_name}"
                    popped_val = temp_buffer_for_next_state[func_key].popleft()
                    popped_ohlcv_data[func_name] = popped_val



                popped_items_for_logging.append({ticker_symbol: popped_ohlcv_data})
                # print("POPPED VECTOR ", popped_ohlcv_data)

                temp_buffer_for_next_state[f"ticker={ticker_symbol}&function=TIMESTAMP"].append(new_timestamp)

                # print("KEYS FOR DATA_FOR_TICKER", data_for_ticker) 
                
                
                
                most_recent_price = float(data_for_ticker['close'])
                most_recent_volume = float(data_for_ticker['volume'])
           
                open_buffer_key = f"ticker={ticker_symbol}&function=OPEN"
                self.hot_ohlcv_realtime_handler[open_buffer_key] = most_recent_price 
                temp_buffer_for_next_state[open_buffer_key].append(most_recent_price)
                


                high_buffer_key = f"ticker={ticker_symbol}&function=HIGH"
                self.hot_ohlcv_realtime_handler[high_buffer_key] = most_recent_price 
                temp_buffer_for_next_state[high_buffer_key].append(most_recent_price)


                low_buffer_key = f"ticker={ticker_symbol}&function=LOW"
                self.hot_ohlcv_realtime_handler[low_buffer_key] = most_recent_price 
                temp_buffer_for_next_state[low_buffer_key].append(most_recent_price)


                close_buffer_key = f"ticker={ticker_symbol}&function=CLOSE"
                self.hot_ohlcv_realtime_handler[close_buffer_key] = most_recent_price 
                temp_buffer_for_next_state[close_buffer_key].append(most_recent_price)

        
                volume_buffer_key = f"ticker={ticker_symbol}&function=VOLUME"
                if self.first_update[ticker_symbol]: 
                    self.first_update[ticker_symbol] = False
                    print(f"UPDATE: intiating first update in place after cold start {most_recent_price} |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
                    temp_buffer_for_next_state[volume_buffer_key][-1] =  (self.hot_ohlcv_realtime_handler[volume_buffer_key] )
                    self.hot_ohlcv_realtime_handler[volume_buffer_key] = most_recent_volume
                    print(f"set  |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
                else: 
                    self.hot_ohlcv_realtime_handler[volume_buffer_key] = most_recent_volume

                    print(f"UPDATE: new minute {most_recent_price}  |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
                    lagging_vol = self.hot_ohlcv_realtime_handler[volume_buffer_key]
                    self.hot_ohlcv_realtime_handler[volume_buffer_key] = most_recent_volume 
                    temp_buffer_for_next_state[volume_buffer_key].append(0)
                    print(f"set  |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
        

                
                

   
            elif ticker_symbol in self.tickers and not update:
                most_recent_price = float(data_for_ticker['close'])
                most_recent_volume = float(data_for_ticker['volume'])
           
           
                high_buffer_key = f"ticker={ticker_symbol}&function=HIGH"
                if most_recent_price > self.hot_ohlcv_realtime_handler[high_buffer_key]: 
                    self.hot_ohlcv_realtime_handler[high_buffer_key] = most_recent_price 
                temp_buffer_for_next_state[high_buffer_key][-1] = (self.hot_ohlcv_realtime_handler[high_buffer_key])


                low_buffer_key = f"ticker={ticker_symbol}&function=LOW"
                if most_recent_price < self.hot_ohlcv_realtime_handler[low_buffer_key]: 
                    self.hot_ohlcv_realtime_handler[low_buffer_key] = most_recent_price
                temp_buffer_for_next_state[low_buffer_key][-1] = (self.hot_ohlcv_realtime_handler[low_buffer_key])


                close_buffer_key = f"ticker={ticker_symbol}&function=CLOSE"
                self.hot_ohlcv_realtime_handler[close_buffer_key] = most_recent_price 
                temp_buffer_for_next_state[close_buffer_key][-1] = (self.hot_ohlcv_realtime_handler[close_buffer_key])

                
                volume_buffer_key = f"ticker={ticker_symbol}&function=VOLUME"
                if self.first_update[ticker_symbol]: 
                    self.first_update[ticker_symbol] = False
                    
                    # print(f"SWAP HOT: intiating first update {most_recent_price}  |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
                    temp_buffer_for_next_state[volume_buffer_key][-1] =  (self.hot_ohlcv_realtime_handler[volume_buffer_key] )
                    self.hot_ohlcv_realtime_handler[volume_buffer_key] = most_recent_volume
                    # print(f"SWAP HOT: set  |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")


                else: 
                    # print(f"SWAP HOT: intiating update in place {most_recent_price}   |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
                    lagging_vol = self.hot_ohlcv_realtime_handler[volume_buffer_key]
                    self.hot_ohlcv_realtime_handler[volume_buffer_key] = most_recent_volume 
                    temp_buffer_for_next_state[volume_buffer_key][-1] =  temp_buffer_for_next_state[volume_buffer_key][-1] +  (most_recent_volume - lagging_vol )
                    

                    # print(f"SWAP HOT: set  |  buffer vol: {temp_buffer_for_next_state[volume_buffer_key][-1] } |  realtime volume: {self.hot_ohlcv_realtime_handler[volume_buffer_key]}")
            else:
                print("ticker error")
                pass 

        for ticker_symbol in self.tickers:
            for func_name, av_key in [
                        ('TIMESTAMP', 'TIMESTAMP'), ('OPEN', 'open'), ('HIGH', 'high'), ('LOW', 'low'),
                        ('CLOSE', 'close'), ('VOLUME', 'volume')
                    ]:  
                        buffer_key = f"ticker={ticker_symbol}&function={func_name}"
                        # print(f"{ticker_symbol}, {func_name}, latest value: {temp_buffer_for_next_state[buffer_key][-1]}")
            
               
        
        # Print items that were "pushed out"
        # if popped_items_for_logging:
        #     print("\n--- Items Pushed Out Due to Max Length ---")
        #     print(f"  Values: {popped_items_for_logging}")
        #     print("------------------------------------------")
       
        
        
        
        self.hot_raw_ohlcv_data_buffer = temp_buffer_for_next_state 


        all_new_indicator_values = {}
        # Make sure self.params contains keys like "ticker=AMD&function=OPEN", "ticker=NVDA&function=RSI"
        for param_str in self.params:
            # _calculate_param_slice must operate on the `temp_buffer_for_next_state`
            result_series = self._calculate_param_slice(param_str, temp_buffer_for_next_state)
            unnormalized_series = result_series[-self.context_depth:] 

            fn_name = param_str.split("function=")[1].split("&")[0] if "function=" in param_str else param_str # Handle if param_str is just a function name
            
            last_value = unnormalized_series.iloc[-1]
            # print(f"LAST VALUE: {param_str[7:30]} {last_value}")
            if last_value is None:
                print(f"Warning: last_value is NaN for {param_str}. Normalization might produce NaNs.")
                normalized_series = pd.Series(np.nan, index=result_series.index[-self.context_depth:])
            else:
                if fn_name in AGAUSSIAN:
                    normalized_series = unnormalized_series - last_value
                elif fn_name in PERCENTILE:
                    normalized_series = (unnormalized_series - last_value) / 100.0
                elif fn_name in CONSTANT:
                    normalized_series = unnormalized_series - last_value
                else: # HUGE functions or default case
                    normalized_series = (unnormalized_series - last_value) / 10000000.0
            
            all_new_indicator_values[param_str] = normalized_series

        ### --- Step 4: Order Slices and Create Final NumPy Tensor --- ###
        
        ordered_series_list = []
        for param_str in self.params:
            # Ensure each series added has the correct length (context_depth)
            series = all_new_indicator_values.get(param_str, pd.Series(np.nan, index=pd.to_datetime([new_timestamp] * self.context_depth))) # Default to NaNs if not found
            if len(series) < self.context_depth:
                # Pad with NaNs at the beginning if cold start hasn't filled all deques yet
                padded_series = pd.Series(np.nan, index=pd.to_datetime([new_timestamp - timedelta(minutes=self.context_depth - 1 - i) for i in range(self.context_depth)]))
                padded_series[-len(series):] = series.values # Fill from the end
                series = padded_series
            ordered_series_list.append(series)
            
        inference_df = pd.concat(ordered_series_list, axis=1)
        inference_df.columns = self.params 
        
    

        inference_item = inference_df.values.astype(np.float32)
        
        # ### --- Step 5: Calculate and Print Statistics for each column --- ###
        # print("\n--- Final Tensor Statistics (per feature column) ---")
        
        if inference_item.size > 0 and inference_item.shape[1] > 0:
            num_features = inference_item.shape[1]
            for i in range(num_features):
                param_name = self.params[i]
                feature_column_data = inference_item[:, i]
                
                mean_val = np.nanmean(feature_column_data)
                std_val = np.nanstd(feature_column_data)
                
                if not np.isnan(mean_val):
                    # print(f"  > Feature '{param_name}': Mean={mean_val:.4f}, Std Dev={std_val:.4f}")
                    pass
                else:
                    print(f"  > Feature '{param_name}': Mean=NaN, Std Dev=NaN (all values are NaN)")
        else:
            print("\n--- Final Tensor is empty or has no features, no statistics to display ---")

        return inference_item

    def infer(self, inference_item: np.ndarray) -> Tuple[torch.Tensor, torch.Tensor]:
        """
        Runs inference through both models and returns their logits.
        """
        with torch.no_grad():
            
            # --- THE FIX IS HERE ---
            # 1. Convert the incoming NumPy array to a PyTorch tensor FIRST.
            x_tensor = torch.from_numpy(inference_item)
            
            # 2. Now you can use PyTorch methods like .unsqueeze() and .to() on the new tensor.
            x = x_tensor.unsqueeze(0).unsqueeze(0).to(self.device)
            
            # Get logits from both models
            profit_logits = self.profit_model(x).squeeze()
            accuracy_logits = self.accuracy_model(x).squeeze()
            
            return profit_logits, accuracy_logits
    def visualize_inference_tensor(self, inference_item: np.ndarray, file_name: str):
        """
        Generates and saves a discrete heatmap of the feature tensor.

        Args:
            inference_item (np.ndarray): The (time_steps, num_features) tensor to plot.
            file_name (str): The name of the image file to save (e.g., 'cold_start_heatmap.png').
        """
        # print(f"\nGenerating heatmap of the feature tensor, shape: {inference_item.shape}...")
        
        # We transpose the matrix so features are on the Y-axis and time is on the X-axis
        data_to_plot = inference_item.T  # Shape becomes (num_features, time_steps)

        # Set a dynamic figure size. Height depends on the number of features.
        # Adjust the numbers as needed for your screen/preference.
        height = max(10, len(self.params) / 4) # at least 10 inches tall
        width = 15
        
        fig, ax = plt.subplots(figsize=(width, height))
        
        # Use seaborn to create the heatmap.
        # 'coolwarm' is great for data centered around zero, like your normalized features.
        sns.heatmap(data_to_plot, 
                    ax=ax,
                    yticklabels=self.params, # Use feature names as Y-axis labels
                    xticklabels=False,      # Hide the X-axis labels (too many time steps)
                    cmap='coolwarm',        # Color scheme
                    cbar=True)              # Show the color bar legend

        ax.set_title('Feature Tensor Heatmap at Cold Start', fontsize=16)
        ax.set_xlabel(f'Time Steps ({self.context_depth} mins)')
        ax.set_ylabel('Features')
        
        # Ensure labels aren't cut off
        plt.tight_layout()
        
        try:
            plt.savefig(file_name, dpi=150) # Save the plot to a file
            # print(f"✅ Heatmap saved successfully to '{file_name}'")
        except Exception as e:
            print(f"❌ Failed to save heatmap. Error: {e}")
        
        plt.close() # Close the plot to free up memory
    def visualize_ohlcv_buffers(self, file_name: str = 'ohlcv_buffers_visualization.png'):
        """
        Generates and saves line plots of the raw OHLCV data stored in the
        engine's buffers after cold start.
        """
        print(f"\nGenerating visualization of the OHLCV data buffers...")

        num_tickers = len(self.tickers)
        if num_tickers == 0:
            print("No tickers to visualize.")
            return

        # Create a tall figure with one subplot for each ticker
        fig, axes = plt.subplots(
            nrows=num_tickers,
            ncols=1,
            figsize=(15, 6 * num_tickers), # 6 inches of height per ticker
            squeeze=False # Ensures axes is always a 2D array
        )
        axes = axes.flatten() # Flatten to a 1D array for easy iteration

        for i, ticker in enumerate(self.tickers):
            ax = axes[i]
            
            # --- Extract Data for this Ticker ---
            # Convert deques to lists for plotting
            try:
                close_data = list(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=CLOSE"])
                open_data = list(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=OPEN"])
                high_data = list(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=HIGH"])
                low_data = list(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=LOW"])
                volume_data = list(self.hot_raw_ohlcv_data_buffer[f"ticker={ticker}&function=VOLUME"])
            except KeyError as e:
                print(f"Data not found for {ticker} in buffer, skipping plot. Error: {e}")
                continue

            if not close_data:
                ax.set_title(f'{ticker} OHLCV Buffer Data - NO DATA FOUND', color='red')
                continue
                
            # --- Plot Price Data (OHLC) ---
            ax.plot(close_data, label='Close', color='blue', linewidth=2)
            ax.plot(open_data, label='Open', color='black', linestyle='--', alpha=0.7)
            ax.plot(high_data, label='High', color='green', linestyle=':', alpha=0.6)
            ax.plot(low_data, label='Low', color='red', linestyle=':', alpha=0.6)
            
            ax.set_title(f'{ticker} OHLCV Buffer Data', fontsize=16)
            ax.set_ylabel('Price ($)', color='blue')
            ax.grid(True, linestyle='--', alpha=0.6)

            # --- Plot Volume Data on a Second Y-Axis ---
            ax2 = ax.twinx() # Create a second y-axis that shares the same x-axis
            ax2.bar(range(len(volume_data)), volume_data, label='Volume', color='grey', alpha=0.2)
            ax2.set_ylabel('Volume', color='grey')
            
            # --- Create a single, combined legend for both axes ---
            lines, labels = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines + lines2, labels + labels2, loc='upper left')

        # Adjust layout to prevent titles and labels from overlapping
        plt.tight_layout(pad=3.0)
        
        try:
            plt.savefig(file_name, dpi=150)
            # print(f"✅ OHLCV buffer visualization saved successfully to '{file_name}'")
        except Exception as e:
            print(f"❌ Failed to save OHLCV visualization. Error: {e}")
            
        plt.close() # Close the plot to free up memory

        