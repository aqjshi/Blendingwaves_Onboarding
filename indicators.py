import numpy as np
import pandas as pd

from scipy.signal import hilbert

import logging
import json
from datetime import timezone # Keep this for explicit UTC timezone


from urllib.parse import parse_qs
def load_indicator_input(path="LOCAL_indic_input.json"):
    logging.info("Loading indicator inp")
    with open(path) as f:
        raw = json.load(f)
    return {k: (v if isinstance(v, list) else [v]) for k, v in raw.items()}



def load_indicator_output(path="LOCAL_indic_output.json"):
    logging.info("Loading indicator outp")
    with open(path) as f:
        raw = json.load(f)
    return {k: (v if isinstance(v, list) else [v]) for k, v in raw.items()}


def parse_param_to_inputs(param_str: str):
    raw = parse_qs(param_str)
    input_dict = {}

    for k, v_list in raw.items():
        val_str = v_list[0]

        # Skip the “function” and “ticker” keys entirely
        if k.lower() in ("function", "ticker"):
            continue

        # If the parameter name contains "period", cast to int
        if "period" in k.lower():
            try:
                input_dict[k] = int(float(val_str))
            except ValueError:
                raise ValueError(f"Expected integer for '{k}', got {val_str!r}")
        else:
            # Otherwise, if it looks numeric, cast to float; else leave as string
            try:
                input_dict[k] = float(val_str)
            except ValueError:
                input_dict[k] = val_str

    fn_name = raw.get("function", [None])[0]
    if fn_name is None:
        raise ValueError(f"Missing 'function' in {param_str!r}")
    key = [fn_name]
    return input_dict, key


def local_OPEN(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Just returns the 'open' series, renamed to key[0].
    """
    return df["OPEN"].rename(key[0])

def local_HIGH(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Just returns the 'high' series, renamed to key[0].
    """
    return df["HIGH"].rename(key[0])

def local_LOW(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Just returns the 'low' series, renamed to key[0].
    """
    return df["LOW"].rename(key[0])

def local_CLOSE(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Just returns the 'close' series, renamed to key[0].
    """
    return df["CLOSE"].rename(key[0])

def local_VOLUME(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Just returns the 'volume' series, renamed to key[0].
    """
    return df["VOLUME"].rename(key[0])

def local_SMA(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Simple Moving Average over `series_type` with window `time_period`.
    """
    series = df[input_dict["series_type"]]
    period = input_dict["time_period"]
    result = series.rolling(window=period, min_periods=period).mean().rename(key[0])
    return result

def local_EMA(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Exponential Moving Average over `series_type` with span `time_period`.
    """
    series = df[input_dict["series_type"]]
    period = input_dict["time_period"]
    result = series.ewm(span=period, adjust=False).mean().rename(key[0])
    return result



def local_WMA(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Weighted Moving Average: weights 1…N over a rolling window,
    implemented with a single convolution for maximum speed.
    """
    series = df[input_dict["series_type"]]
    period = int(input_dict["time_period"])
    if period < 1:
        raise ValueError(f"time_period must be >=1, got {period}")

    # 1) Precompute weights and their sum
    weights = np.arange(1, period + 1, dtype=float)
    wsum = weights.sum()

    # 2) Extract raw values
    vals = series.to_numpy(dtype=float)

    # 3) Convolve with reversed weights → numerator of WMA
    #    mode='valid' yields only those positions where the full window overlaps
    numer = np.convolve(vals, weights[::-1], mode="valid")

    # 4) Build output array, pad the first (period-1) entries with NaN
    wma = np.empty_like(vals)
    wma[:period-1] = np.nan
    wma[period-1:] = numer / wsum

    # 5) Return as a Series aligned to the original index
    return pd.Series(wma, index=series.index, name=key[0])

def local_DEMA(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Double Exponential MA: 2*EMA1 − EMA2
    """
    # first EMA
    ema1 = df[input_dict["series_type"]].ewm(
        span=input_dict["time_period"], adjust=False
    ).mean()
    # second EMA on the first
    ema2 = ema1.ewm(span=input_dict["time_period"], adjust=False).mean()
    return (2 * ema1 - ema2).rename(key[0])

def local_TEMA(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Triple EMA: 3*EMA1 − 3*EMA2 + EMA3
    """
    series = df[input_dict["series_type"]]
    period = input_dict["time_period"]
    ema1 = series.ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    ema3 = ema2.ewm(span=period, adjust=False).mean()
    return (3*ema1 - 3*ema2 + ema3).rename(key[0])

def local_TRIMA(df: pd.DataFrame, input_dict: dict, key: list):
    series = df[input_dict["series_type"]]
    period = input_dict["time_period"]
    # first SMA over window N
    sma1 = series.rolling(window=period, min_periods=period).mean()
    # second SMA over the same window
    trima = sma1.rolling(window=period, min_periods=period).mean()
    return trima.rename(key[0])


def local_T3(df: pd.DataFrame, input_dict: dict, key: list):
    """
    T3 Moving Average (HMA triple-smooth). Approximate via multiple EMA passes and a volume factor.
    """
    series = df[input_dict["series_type"]]
    period = input_dict["time_period"]
    vol = 0.7  # common default; could be parameterized
    e1 = series.ewm(span=period, adjust=False).mean()
    e2 = e1.ewm(span=period, adjust=False).mean()
    e3 = e2.ewm(span=period, adjust=False).mean()
    e4 = e3.ewm(span=period, adjust=False).mean()
    e5 = e4.ewm(span=period, adjust=False).mean()
    e6 = e5.ewm(span=period, adjust=False).mean()
    c1 = -vol**3
    c2 = 3*vol**2 + 3*vol**3
    c3 = -6*vol**2 - 3*vol - 3*vol**3
    c4 = 1 + 3*vol + vol**3 + 3*vol**2
    t3 = c1*e6 + c2*e5 + c3*e4 + c4*e3
    return t3.rename(key[0])

def local_FAMA(df: pd.DataFrame, tpl: dict, key: list):
    """
    Pure‑Python approximation for MESA Adaptive MA (MAMA) & FAMA.

    Uses two EMAs with α=fastlimit and α=slowlimit as stand‑ins:
      MAMA ≈ EMA(data, α=fastlimit)
      FAMA ≈ EMA(data, α=slowlimit)
    """
    series = df[tpl["series_type"]]
    slow = float(tpl.get("slowlimit", 0.03))   # same for slowlimit

    fama = series.ewm(alpha=slow,  adjust=False).mean()
    # if you also need FAMA, you can compute it here with alpha=slow

    return pd.Series(fama, index=series.index, name=key[0])



def local_MAMA(df: pd.DataFrame, tpl: dict, key: list) -> pd.Series:
    """
    Pure‑Python approximation for MESA Adaptive MA (MAMA) & FAMA.

    Uses two EMAs with α=fastlimit and α=slowlimit as stand‑ins:
      MAMA ≈ EMA(data, α=fastlimit)
      FAMA ≈ EMA(data, α=slowlimit)
    """
    series = df[tpl["series_type"]]
    fast = float(tpl.get("fastlimit", 0.05))   # default to 0.05 if not provided
    mama = series.ewm(alpha=fast,  adjust=False).mean()
    # if you also need FAMA, you can compute it here with alpha=slow

    return pd.Series(mama, index=series.index, name=key[0])




def intended_ma(encoded_int):
    """
    Maps an integer code to a moving average function.
    0 = SMA, 1 = EMA, 2 = WMA, 3 = DEMA, 4 = TEMA, 5 = TRIMA, 6 = T3, 7 = FAMA, 8 = MAMA
    """
    ma_funcs = [
        local_SMA,   # 0
        local_EMA,   # 1
        local_WMA,   # 2
        local_DEMA,  # 3
        local_TEMA,  # 4
        local_TRIMA, # 5
        local_T3,    # 6
        local_FAMA,  # 7
        local_MAMA   # 8
    ]
    if 0 <= encoded_int < len(ma_funcs):
        return ma_funcs[encoded_int]
    else:
        raise ValueError(f"Invalid moving average code: {encoded_int}")
    
def local_VWAP(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:

    
    # Ensure columns exist and are numeric
    required_cols = ["HIGH", "LOW", "CLOSE", "VOLUME"]
    df_processed = df.copy() # Work on a copy to avoid modifying original df
    df_processed.columns = df_processed.columns.str.upper() # Standardize column names

    for col in required_cols:
        if col not in df_processed.columns:
            print(f"Warning: Missing required column '{col}' for VWAP calculation. Returning all-NaN series.")
            return pd.Series(np.nan, index=df.index, name=key[0])
        # Ensure columns are numeric, fill NaNs as needed (already done in _calculate_param_slice typically)
        df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').ffill().bfill()
    
    # Fill volume NaNs with 0 before calculations
    df_processed['VOLUME'] = df_processed['VOLUME'].fillna(0)

    # Calculate Typical Price (TP)
    tp = (df_processed["HIGH"] + df_processed["LOW"] + df_processed["CLOSE"]) / 3
    
    # Calculate TP * Volume
    tp_vol = tp * df_processed["VOLUME"]

    # Calculate cumulative sums over the entire DataFrame
    # This assumes no daily reset.
    cum_tp_vol = tp_vol.cumsum()
    cum_vol = df_processed["VOLUME"].cumsum()

    # Intraday VWAP (or rather, cumulative VWAP)
    # Handle division by zero: if cum_vol is 0, vwap should be NaN or 0
    vwap = cum_tp_vol / cum_vol
    
    # Replace any potential infinite values with NaN that could arise from 0/0 or X/0.
    vwap = vwap.replace([np.inf, -np.inf], np.nan)
    
    # Fill any remaining NaNs (e.g., if first few volumes were zero)
    # This might fill initial NaNs with 0, which is a choice.
    vwap = vwap.ffill()


    # Return the VWAP series, named to match key[0] and with the original df's index
    return vwap.rename(key[0])

def local_MACD_MACD(df: pd.DataFrame, input_dict: dict, key: list):
    series     = df[input_dict["series_type"]]
    fastperiod = input_dict["fastperiod"]
    slowperiod = input_dict["slowperiod"]

    # compute the two EMAs
    ema_fast = series.ewm(span=fastperiod, adjust=False).mean()
    ema_slow = series.ewm(span=slowperiod, adjust=False).mean()
    macd_line = ema_fast - ema_slow

    # turn into a DataFrame column named key[0]
    out = macd_line.to_frame(name=key[0])

    # if every value is NaN, log a warning
    if out[key[0]].isna().all():
        print(f"Warning: local_MACD_MACD produced all‐NaN for key={key[0]}")

    return out


def local_MACD_SIGNAL(df: pd.DataFrame, input_dict: dict, key: list):
    series       = df[input_dict["series_type"]]
    fastperiod   = input_dict["fastperiod"]
    slowperiod   = input_dict["slowperiod"]
    signalperiod = input_dict["signalperiod"]

    ema_fast  = series.ewm(span=fastperiod, adjust=False).mean()
    ema_slow  = series.ewm(span=slowperiod, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signalperiod, adjust=False).mean()

    out = signal_line.to_frame(name=key[0])

    if out[key[0]].isna().all():
        print(f"Warning: local_MACD_SIGNAL produced all‐NaN for key={key[0]}")

    return out


def local_MACD_HIST(df: pd.DataFrame, input_dict: dict, key: list):
    series       = df[input_dict["series_type"]]
    fastperiod   = input_dict["fastperiod"]
    slowperiod   = input_dict["slowperiod"]
    signalperiod = input_dict["signalperiod"]

    ema_fast   = series.ewm(span=fastperiod, adjust=False).mean()
    ema_slow   = series.ewm(span=slowperiod, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signalperiod, adjust=False).mean()
    hist = macd_line - signal_line

    out = hist.to_frame(name=key[0])

    if out[key[0]].isna().all():
        print(f"Warning: local_MACD_HIST produced all‐NaN for key={key[0]}")

    return out

def local_WILLR(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Williams %R:
      WILLR_t = (HighestHigh_N − Close_t) / (HighestHigh_N − LowestLow_N) * -100
    where N = time_period.
    """
    period = input_dict["time_period"]
    high   = df["HIGH"]
    low    = df["LOW"]
    close  = df["CLOSE"]

    hh = high.rolling(window=period, min_periods=period).max()
    ll = low.rolling( window=period, min_periods=period).min()

    willr = (hh - close) / (hh - ll) * -100

    return willr.rename(key[0])

def local_ADX(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Average Directional Index (ADX):
      1) +DM = today.high - yesterday.high if > (yest.low - today.low) and >0 else 0
      2) -DM = yesterday.low - today.low if > (today.high - yest.high) and >0 else 0
      3) TR = max( high - low, abs(high - prev_close), abs(low - prev_close) )
      4) Smooth +DM, -DM, TR by Wilder’s method over N periods
      5) DI+ = 100 * smoothed +DM / ATR, DI− similarly
      6) DX = 100 * |DI+ − DI−| / (DI+ + DI−)
      7) ADX = Wilder’s smooth of DX over N periods
    """
    period = input_dict["time_period"]
    high   = df["HIGH"]
    low    = df["LOW"]
    close  = df["CLOSE"]

    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    prev_close = close.shift(1)

    up_move   = high - prev_high
    down_move = prev_low - low

    plus_dm  = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low  - prev_close).abs()
    tr  = pd.Series(np.maximum.reduce([tr1, tr2, tr3]), index=df.index)

    # Wilder smoothing
    atr      = tr.ewm(alpha=1/period, adjust=False).mean()
    smooth_p = pd.Series(plus_dm,  index=df.index).ewm(alpha=1/period, adjust=False).mean()
    smooth_m = pd.Series(minus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean()

    di_plus  = 100 * smooth_p / atr
    di_minus = 100 * smooth_m / atr

    dx = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus)
    adx = dx.ewm(alpha=1/period, adjust=False).mean()

    return adx.rename(key[0])

def local_ADXR(df: pd.DataFrame, input_dict: dict, key: list):
    """
    ADXR (Average Directional Movement Rating):
      ADXR_t = (ADX_t + ADX_{t−N}) / 2,
    where ADX is as above, and N = time_period.
    """
    # first compute ADX
    adx = local_ADX(df, input_dict, [key[0]]).rename("ADX")

    period = input_dict["time_period"]
    adx_lag = adx.shift(period)

    adxr = (adx + adx_lag) / 2

    return adxr.rename(key[0])


def _extract_first(out):
    if isinstance(out, pd.DataFrame):
        return out.iloc[:, 0]
    return out


def local_MACDEXT_MACD(df: pd.DataFrame, input_dict: dict, keys: list):
    """
    MACD line = EMA(fastperiod) − EMA(slowperiod)
    """
    series = df[input_dict["series_type"]]
    fast_period = int(input_dict["fastperiod"])
    slow_period = int(input_dict["slowperiod"])

    fast_ema = series.ewm(span=fast_period, adjust=False).mean()
    slow_ema = series.ewm(span=slow_period, adjust=False).mean()
    macd    = fast_ema - slow_ema

    macd.name = keys[0]
    return macd.to_frame()


def local_MACDEXT_SIGNAL(df: pd.DataFrame, input_dict: dict, keys: list):
    """
    Signal line = EMA(signalperiod) of the MACD line
    """
    # first build the MACD series
    macd_df   = local_MACDEXT_MACD(df, input_dict, ["_MACD_tmp"])
    macd_line = macd_df["_MACD_tmp"]

    sig_period = int(input_dict["signalperiod"])
    signal     = macd_line.ewm(span=sig_period, adjust=False).mean()

    signal.name = keys[0]
    return signal.to_frame()


def local_MACDEXT_HIST(df: pd.DataFrame, input_dict: dict, keys: list):
    """
    Histogram = MACD line − Signal line
    """
    macd_df   = local_MACDEXT_MACD(df, input_dict, ["_MACD_tmp"])
    macd_line = macd_df["_MACD_tmp"]

    sig_df    = local_MACDEXT_SIGNAL(df, input_dict, ["_SIGNAL_tmp"])
    sig_line  = sig_df["_SIGNAL_tmp"]

    hist = macd_line - sig_line
    hist.name = keys[0]
    return hist.to_frame()


def _compute_slow_stoch(df: pd.DataFrame, input_dict: dict):
    """
    Returns a DataFrame with columns ['pctK', 'SlowK'] for the slow stochastic.
    """
    high, low, close = df["HIGH"], df["LOW"], df["CLOSE"]
    K = input_dict["fastkperiod"]
    hh = high.rolling(K).max()
    ll = low.rolling(K).min()
    pctK = 100 * (close - ll) / (hh - ll)

    # SlowK
    slowK_ma = intended_ma(int(input_dict["slowkmatype"]))
    tplK = {"series_type": "pctK", "time_period": input_dict["slowkperiod"]}
    slowK = _extract_first(slowK_ma(pctK.to_frame("pctK"), tplK, ["SlowK"]))

    return pd.DataFrame({
        "pctK": pctK,
        "SlowK": slowK
    }, index=df.index)


def local_STOCH_SLOWK(df: pd.DataFrame, input_dict: dict, key: list):
    # print("hit first")
    """
    key = [<output column>]
    returns only SlowK under key[0]
    """
    slow = _compute_slow_stoch(df, input_dict)
    return slow[["SlowK"]].rename(columns={"SlowK": key[0]})


def local_STOCH_SLOWD(df: pd.DataFrame, input_dict: dict, key: list):
    # print("hit firstD")
    """
    key = [<output column>]
    smooths SlowK and returns SlowD under key[0]
    """
    slow = _compute_slow_stoch(df, input_dict)
    slowD_ma = intended_ma(int(input_dict["slowdmatype"]))
    tplD = {"series_type": "SlowK", "time_period": input_dict["slowdperiod"]}

    # rename required so series_type resolves
    slowK_df = slow[["SlowK"]]
    slowD = _extract_first(slowD_ma(slowK_df, tplD, [key[0]]))

    return pd.DataFrame({key[0]: slowD}, index=df.index)




def wilder_rsi(series: pd.Series, period: int) -> pd.Series:
    """
    Pure pandas version of Wilder’s RSI:
    1) delta = diff(series)
    2) up = positive deltas, down = absolute negative deltas
    3) rolling EMA on up/down with alpha=1/period (Wilder’s smoothing)
    4) RSI = 100 - 100/(1 + RS)
    """
    delta = series.diff()

    # gains/losses
    up   = delta.clip(lower=0)
    down = -delta.clip(upper=0)

    # Wilder’s smoothing is just an EWM with alpha=1/period, adjust=False
    roll_up   = up.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

    rs  = roll_up / roll_down
    rsi = 100 - 100 / (1 + rs)

    return rsi

def local_RSI(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Wilder’s RSI via pure pandas EWM (no loops).
    """
    series = df[input_dict["series_type"]]
    period = int(input_dict["time_period"])
    rsi    = wilder_rsi(series, period)
    return rsi.rename(key[0])


def _compute_stochrsi_pctK_and_grp(df: pd.DataFrame, input_dict: dict):
    # Ensure local_RSI is correctly implemented and returns a Series with df.index
    rsi = local_RSI(df, input_dict, ["RSI"]) 

    FK = input_dict["fastkperiod"]


    # No groupby needed. Rolling min/max will be applied to the entire RSI series.
    lo = rsi.rolling(FK, min_periods=FK).min()
    hi = rsi.rolling(FK, min_periods=FK).max()
    # --- CHANGE END ---

    denominator = (hi - lo)
    
    # Calculate pctK
    pctK_raw = 100 * (rsi - lo) / denominator
    
    # Replace infinite values (from division by zero) with NaN
    pctK = pctK_raw.replace([np.inf, -np.inf], np.nan)

    # Fill NaNs. Be aware that this fills *all* NaNs, including those from insufficient data.
    pctK = pctK.fillna(0.0) 
    return pctK, None

def local_STOCHRSI_FASTK(df, input_dict, key):
    pctK, grp = _compute_stochrsi_pctK_and_grp(df, input_dict)
    return pd.DataFrame({ key[0]: pctK }, index=df.index)


def local_STOCHRSI_FASTD(df: pd.DataFrame, input_dict: dict, key: list): # Add type hints for clarity
    pctK, grp = _compute_stochrsi_pctK_and_grp(df, input_dict)

    # 3) FastD smoothing
    FD = input_dict.get("fastdperiod", 3)
    ma_fastd = intended_ma(int(input_dict.get("fastdmatype", 0)))
    
    # --- CHANGE START: REMOVE GROUPBY IN FASTD SMOOTHING ---
    # Apply MA directly to pctK Series, as there are no longer groups
    fastD = ma_fastd(pctK.to_frame("pctK"), {"series_type": "pctK", "time_period": FD}, ["FastD"])
    # --- CHANGE END ---

    if isinstance(fastD, pd.DataFrame):
        fastD = fastD.iloc[:, 0] # Extract Series if ma_fastd returns DataFrame

    return pd.DataFrame({ key[0]: fastD }, index=df.index)



def local_APO(df: pd.DataFrame, input_dict: dict, key: list) -> pd.DataFrame:

    col_lc = input_dict.get("series_type", "").lower()
    matches = [c for c in df.columns if c.lower() == col_lc]
    if not matches:
        # Missing source column → return all‐NaN
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)
    real_col = matches[0]

    # 2) Extract as float and forward‐fill only
    src = df[real_col].astype(float).ffill()
    if src.isna().all():
        # All values are NaN even after forward‐fill → no computation possible
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    # 3) Build helper DataFrame for the MA routines
    temp = src.to_frame(name="__aposer__")

    # 4) Choose the MA function
    try:
        ma_fn = intended_ma(int(input_dict.get("matype", 0)))
    except Exception:
        # Invalid matype → return all‐NaN
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    # 5) Prepare param dicts for fast/slow MA
    try:
        fast_period = int(input_dict.get("fastperiod", 0))
    except (TypeError, ValueError):
        fast_period = 0
    try:
        slow_period = int(input_dict.get("slowperiod", 0))
    except (TypeError, ValueError):
        slow_period = 0

    fast_tpl = {
        **input_dict,
        "series_type": "__aposer__",
        "time_period": fast_period,
    }
    slow_tpl = {
        **input_dict,
        "series_type": "__aposer__",
        "time_period": slow_period,
    }

    # 6) Compute the moving averages; protect against MA errors
    try:
        fast_ma = _extract_first(ma_fn(temp, fast_tpl, ["fastperiod"]))
        # ensure alignment and float dtype
        fast_ma = fast_ma.reindex(df.index).astype(float)
    except Exception:
        fast_ma = pd.Series([np.nan] * len(df), index=df.index)

    try:
        slow_ma = _extract_first(ma_fn(temp, slow_tpl, ["slowperiod"]))
        slow_ma = slow_ma.reindex(df.index).astype(float)
    except Exception:
        slow_ma = pd.Series([np.nan] * len(df), index=df.index)

    # 7) Subtract to form APO; missing values propagate as NaN
    apo = fast_ma.subtract(slow_ma, fill_value=np.nan)

    # 8) Return as DataFrame
    return pd.DataFrame({key[0]: apo}, index=df.index)


def local_PPO(df: pd.DataFrame, input_dict: dict, key: list) -> pd.DataFrame:
    """
    Percentage Price Oscillator (PPO):
      1) Compute two MAs (fast and slow) on the chosen series
      2) PPO = 100 * (fastMA - slowMA) / slowMA

    Expects input_dict to contain:
      - "series_type": the column name (case‐insensitive)
      - "fastperiod": integer period for the fast MA
      - "slowperiod": integer period for the slow MA
      - "matype": integer code for which MA function to use (intended_ma)

    Returns a one‐column DataFrame with the PPO series named key[0].
    """

    # 1) Normalize series_type to lowercase & find matching column
    col_lc = input_dict.get("series_type", "").lower()
    matches = [c for c in df.columns if c.lower() == col_lc]
    if not matches:
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)
    real_col = matches[0]

    # 2) Extract as float and forward‐fill only
    src = df[real_col].astype(float).ffill()
    if src.isna().all():
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    # 3) Build helper DataFrame for the MA routines
    temp = src.to_frame(name="__pposer__")

    # 4) Choose the MA function
    try:
        ma_fn = intended_ma(int(input_dict.get("matype", 0)))
    except Exception:
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    # 5) Prepare param dicts for fast/slow MA
    try:
        fast_period = int(input_dict.get("fastperiod", 0))
    except (TypeError, ValueError):
        fast_period = 0
    try:
        slow_period = int(input_dict.get("slowperiod", 0))
    except (TypeError, ValueError):
        slow_period = 0

    fast_tpl = {
        **input_dict,
        "series_type": "__pposer__",
        "time_period": fast_period,
    }
    slow_tpl = {
        **input_dict,
        "series_type": "__pposer__",
        "time_period": slow_period,
    }

    # 6) Compute the moving averages; protect against MA errors
    try:
        fast_ma = _extract_first(ma_fn(temp, fast_tpl, ["fastperiod"]))
        fast_ma = fast_ma.reindex(df.index).astype(float)
    except Exception:
        fast_ma = pd.Series([np.nan] * len(df), index=df.index)

    try:
        slow_ma = _extract_first(ma_fn(temp, slow_tpl, ["slowperiod"]))
        slow_ma = slow_ma.reindex(df.index).astype(float)
    except Exception:
        slow_ma = pd.Series([np.nan] * len(df), index=df.index)

    # 7) Compute PPO with guard against division errors, forward‐propagate NaNs
    with np.errstate(divide='ignore', invalid='ignore'):
        diff = fast_ma.subtract(slow_ma, fill_value=np.nan)
        denom = slow_ma.replace(0, np.nan)
        ppo = diff.mul(100).div(denom)

    # 8) Return as DataFrame
    return pd.DataFrame({key[0]: ppo}, index=df.index)
def local_MOM(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Momentum (MOM):
      MOM_t = series_t − series_{t−N}
    """
    series = df[input_dict["series_type"]]
    N      = input_dict["time_period"]
    mom    = series.diff(N)
    return mom.rename(key[0])


def local_BOP(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Balance of Power (BOP):
      BOP_t = (Close_t − Open_t) / (High_t − Low_t)
    """
    op    = df["OPEN"]
    hi    = df["HIGH"]
    lo    = df["LOW"]
    cl    = df["CLOSE"]
    bop   = (cl - op) / (hi - lo)
    return bop.rename(key[0])


def local_CCI(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Commodity Channel Index (CCI):
      TP  = (High + Low + Close) / 3
      M   = TP.rolling(N).mean()
      MD  = TP.rolling(N).mad()          ← C‑optimized mean absolute deviation
      CCI = (TP − M) / (0.015 × MD)
    """
    N   = int(input_dict["time_period"])
    tp  = (df["HIGH"] + df["LOW"] + df["CLOSE"]) / 3

    # rolling mean of TP
    ma_tp = tp.rolling(window=N, min_periods=N).mean()
    # rolling mean absolute deviation (C‑level)
    mad   = tp.rolling(window=N, min_periods=N).max()

    cci = (tp - ma_tp) / (0.015 * mad)
    return cci.rename(key[0])

def local_ROC(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Rate of Change (ROC):
      ROC_t = 100 × (series_t − series_{t−N}) / series_{t−N}
    """
    series = df[input_dict["series_type"]]
    N      = input_dict["time_period"]
    roc    = 100 * (series - series.shift(N)) / series.shift(N)
    return roc.rename(key[0])


def local_ROCR(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Rate of Change Ratio (ROCR):
      ROCR_t = series_t / series_{t−N}
    """
    series = df[input_dict["series_type"]]
    N      = input_dict["time_period"]
    rocr   = series / series.shift(N)
    return rocr.rename(key[0])
def local_MFI(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Money Flow Index (MFI):
      TypicalPrice = (High + Low + Close) / 3
      MoneyFlow    = TypicalPrice × Volume
      PositiveMF   = sum of MoneyFlow where TypicalPrice > prev TypicalPrice
      NegativeMF   = sum of MoneyFlow where TypicalPrice < prev TypicalPrice
      MFI = 100 × PositiveMF / (PositiveMF + NegativeMF)  over period N
    """
    N   = input_dict["time_period"]
    high, low, close, vol = df["HIGH"], df["LOW"], df["CLOSE"], df["VOLUME"]

    tp = (high + low + close) / 3
    mf = tp * vol
    tp_prev = tp.shift(1)

    pos_mf = mf.where(tp > tp_prev, 0.0).rolling(N).sum()
    neg_mf = mf.where(tp < tp_prev, 0.0).rolling(N).sum()

    mfi = 100 * pos_mf / (pos_mf + neg_mf)
    return mfi.rename(key[0])

def local_TRIX(df: pd.DataFrame, input_dict: dict, key: list):
    """
    TRIX:
      1) Triple-smoothed EMA of series over N
      2) TRIX_t = 100 × (EMA3_t − EMA3_{t−1}) / EMA3_{t−1}
    """
    series = df[input_dict["series_type"]]
    N      = input_dict["time_period"]

    ema1 = series.ewm(span=N, adjust=False).mean()
    ema2 = ema1.ewm(span=N, adjust=False).mean()
    ema3 = ema2.ewm(span=N, adjust=False).mean()

    trix = 100 * (ema3 - ema3.shift(1)) / ema3.shift(1)
    return trix.rename(key[0])


# def _directional_movements(df: pd.DataFrame):
#     high       = df["HIGH"]
#     low        = df["LOW"]
#     close      = df["CLOSE"]
#     prev_high  = high.shift(1)
#     prev_low   = low.shift(1)
#     prev_close = close.shift(1)

#     up_move   = high - prev_high
#     down_move = prev_low - low

#     plus_dm  = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
#     minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

#     tr1 = high - low
#     tr2 = (high - prev_close).abs()
#     tr3 = (low  - prev_close).abs()
#     tr  = pd.Series(np.maximum.reduce([tr1, tr2, tr3]), index=df.index)

#     return pd.Series(plus_dm,  index=df.index), pd.Series(minus_dm, index=df.index), tr

def _directional_movements(df: pd.DataFrame):
    """
    Helper: computes raw +DM, -DM, and True Range as in the
    classical Directional Movement system.
    """
    high       = df["HIGH"]
    low        = df["LOW"]
    close      = df["CLOSE"]
    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    prev_close = close.shift(1)

    up_move   = high - prev_high
    down_move = prev_low - low

    plus_dm  = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low  - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return pd.Series(plus_dm, index=df.index), \
           pd.Series(minus_dm, index=df.index), \
           true_range



def local_PLUS_DI(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Plus Directional Indicator: 100 * smoothed(+DM) / smoothed(TR)
    """
    period  = input_dict["time_period"]
    plus_dm, _, tr = _directional_movements(df)

    # Wilder smoothing via EMA α=1/period
    smooth_p = plus_dm.ewm(alpha=1/period, adjust=False).mean()
    atr      = tr     .ewm(alpha=1/period, adjust=False).mean()

    plus_di = 100 * smooth_p / atr
    return plus_di.rename(key[0])

def local_MINUS_DI(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Minus Directional Indicator: 100 * smoothed(−DM) / smoothed(TR)
    """
    period  = input_dict["time_period"]
    _, minus_dm, tr = _directional_movements(df)

    smooth_m = minus_dm.ewm(alpha=1/period, adjust=False).mean()
    atr      = tr     .ewm(alpha=1/period, adjust=False).mean()

    minus_di = 100 * smooth_m / atr
    return minus_di.rename(key[0])

def local_DX(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Directional Movement Index (DX):
      +DM_t = max(High_t – High_{t-1}, 0) if it's > (Low_{t-1} – Low_t), else 0
      –DM_t = max(Low_{t-1} – Low_t, 0) if it's > (High_t – High_{t-1}), else 0
      TR_t  = max(High_t–Low_t, |High_t–Close_{t-1}|, |Low_t–Close_{t-1}|)
      +DI_t = 100 * EMA(+DM, α=1/N) / EMA(TR, α=1/N)
      –DI_t = 100 * EMA(–DM, α=1/N) / EMA(TR, α=1/N)
      DX_t  = 100 * |+DI_t – –DI_t| / (+DI_t + –DI_t)
    """
    period = int(input_dict["time_period"])
    alpha  = 1.0 / period

    hi, lo, cl = df["HIGH"], df["LOW"], df["CLOSE"]
    prev_hi    = hi.shift(1)
    prev_lo    = lo.shift(1)
    prev_cl    = cl.shift(1)

    # 1) Raw directional movement
    up   = hi - prev_hi
    down = prev_lo - lo
    plus_dm  = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)

    # 2) True range
    tr1 = hi - lo
    tr2 = (hi - prev_cl).abs()
    tr3 = (lo - prev_cl).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 3) Wilder’s smoothing via EWM (α=1/N, adjust=False)
    ema_plus_dm  = plus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    ema_minus_dm = minus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    ema_tr       = tr.ewm(alpha=alpha, adjust=False, min_periods=period).mean()

    # 4) +DI, –DI and then DX
    plus_di  = 100 * ema_plus_dm  / ema_tr
    minus_di = 100 * ema_minus_dm / ema_tr
    dx       = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)

    return dx.rename(key[0])



def local_MIDPOINT(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Midpoint indicator: (HighestHigh_N + LowestLow_N) / 2
    """
    period = input_dict["time_period"]
    hi = df['HIGH'].rolling(window=period, min_periods=period).max()
    lo = df['LOW'].rolling(window=period, min_periods=period).min()
    midpoint = (hi + lo) / 2
    return midpoint.rename(key[0])



def local_MIDPRICE(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Rolling Mid-Price: (Highest High + Lowest Low) / 2 over `time_period` bars.
    """
    hi = df["HIGH"]
    lo = df["LOW"]
    N  = input_dict["time_period"]

    highest = hi.rolling(window=N, min_periods=N).max()
    lowest  = lo.rolling(window=N, min_periods=N).min()
    midprice = (highest + lowest) / 2

    return midprice.rename(key[0])


def local_TRANGE(df: pd.DataFrame, input_dict: dict, key: list):
    """
    True Range: max(high−low, |high−prev_close|, |low−prev_close|)
    """
    high, low, close = df["HIGH"], df["LOW"], df["CLOSE"]
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low  - prev_close).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rename(key[0])


def local_ATR(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Average True Range: Wilder’s EMA of True Range over N
    """
    period = input_dict["time_period"]
    tr     = local_TRANGE(df, input_dict, ["TRANGE"])
    atr    = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr.rename(key[0])


def local_NATR(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Normalized ATR: ATR / Close * 100
    """
    atr   = local_ATR(df, input_dict, ["ATR"])
    natr  = atr / df["CLOSE"] * 100
    return natr.rename(key[0])


def local_SAR(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:
    """
    Compute Parabolic SAR from scratch without external libraries.
    Expects input_dict to contain:
      - "acceleration": initial acceleration factor (e.g. 0.02)
      - "maximum": maximum acceleration factor (e.g. 0.2)
    df must have columns "HIGH", "LOW", and "CLOSE" (case‐insensitive).
    Returns a pandas Series of SAR values, indexed like df, named key[0].
    """
    
    # 1) Robustly check for and prepare columns
    # Ensure column names are consistent (e.g., all uppercase as expected by the logic)
    df_upper_cols = df.copy()
    df_upper_cols.columns = df_upper_cols.columns.str.upper()

    col_high = "HIGH"
    col_low = "LOW"
    col_close = "CLOSE"
    
    required_cols = [col_high, col_low, col_close]
    if not all(col in df_upper_cols.columns for col in required_cols):
        print(f"Warning: Missing required columns for SAR in DataFrame: {', '.join([col for col in required_cols if col not in df_upper_cols.columns])}. Returning all-NaN series.")
        return pd.Series(np.nan, index=df.index, name=key[0])

    # Extract and forward/back-fill to handle intermittent NaNs
    # IMPORTANT: .to_numpy() on a Series with NaNs converts them to np.nan, which is float.
    # If the entire series is NaN after ffill/bfill, it will remain NaN.
    high = df_upper_cols[col_high].astype(float).ffill().bfill().to_numpy()
    low = df_upper_cols[col_low].astype(float).ffill().bfill().to_numpy()
    closes = df_upper_cols[col_close].astype(float).ffill().bfill().to_numpy()
    n = len(df)
    
    # Check for empty DataFrame or all-NaN core data *after* filling
    if n < 2 or np.all(np.isnan(high)) or np.all(np.isnan(low)) or np.all(np.isnan(closes)):
        print(f"Warning: Not enough valid data (n={n}) or all NaNs after fill for SAR computation. Returning all-NaN series.")
        return pd.Series(np.nan, index=df.index, name=key[0])

    # 3) Read acceleration parameters
    try:
        acc = float(input_dict.get("acceleration", 0.02))
        max_af = float(input_dict.get("maximum", 0.2))
    except (ValueError, TypeError): # Catch conversion errors
        print("Warning: Invalid acceleration/maximum parameters for SAR. Using defaults (0.02, 0.2).")
        acc = 0.02
        max_af = 0.2

    # 4) Pre-allocate SAR array
    sar = np.full(n, np.nan, dtype=float)

    # --- ROBUST INITIALIZATION START ---
    # Find the first index `i` where closes[i] and closes[i+1] are NOT NaN
    first_valid_idx = -1
    for k in range(n - 1):
        if not np.isnan(closes[k]) and not np.isnan(closes[k+1]):
            first_valid_idx = k
            break
    
    if first_valid_idx == -1:
        print("Warning: Not enough consecutive valid close prices to determine initial SAR trend. Returning all-NaN series.")
        return pd.Series(np.nan, index=df.index, name=key[0])

    # Use the first_valid_idx for initialization
    # 5) Determine initial trend based on first two *valid* closes
    trend_up = closes[first_valid_idx + 1] >= closes[first_valid_idx]

    # 6) Initialize EP (Extreme Point) and AF (acceleration factor)
    af = acc
    if trend_up:
        ep = high[first_valid_idx]
        sar[first_valid_idx] = low[first_valid_idx]
    else:
        ep = low[first_valid_idx]
        sar[first_valid_idx] = high[first_valid_idx]
    
    if first_valid_idx + 1 < n:
        sar[first_valid_idx + 1] = sar[first_valid_idx]
    
    for i in range(first_valid_idx + 2, n): 
        # Check for NaNs in current bar's data (high[i], low[i], closes[i])
        if np.isnan(high[i]) or np.isnan(low[i]) or np.isnan(closes[i]):
            pass # Keep sar[i] as NaN and continue.

        prev_sar = sar[i - 1]
        prev_ep = ep # ep is updated in the loop based on previous bar
        prev_af = af # af is updated in the loop based on previous bar

        # If previous SAR is NaN, current SAR will also be NaN.
        # This is expected and desirable; NaN propagates.
        if np.isnan(prev_sar) or np.isnan(prev_ep) or np.isnan(prev_af):
             sar[i] = np.nan
             continue # Skip calculations if inputs are NaN

        # 8a) Compute the preliminary SAR
        curr_sar = prev_sar + prev_af * (prev_ep - prev_sar)

        # 8b) Boundary rule: cannot lie inside last two bars’ highs/lows
        # Ensure i-1 and i-2 are within bounds and their data is not NaN
        if trend_up:
            if i - 1 >= 0 and not np.isnan(low[i-1]): # Check previous low for NaN
                curr_sar = min(curr_sar, low[i - 1])
            if i - 2 >= 0 and not np.isnan(low[i-2]): # Check previous-previous low for NaN
                curr_sar = min(curr_sar, low[i - 2])
        else: # Downtrend
            if i - 1 >= 0 and not np.isnan(high[i-1]): # Check previous high for NaN
                curr_sar = max(curr_sar, high[i - 1])
            if i - 2 >= 0 and not np.isnan(high[i-2]): # Check previous-previous high for NaN
                curr_sar = max(curr_sar, high[i - 2])


        # 8c) Check for reversal
        if trend_up:
            if not np.isnan(low[i]) and low[i] < curr_sar: # Check current low for NaN
                # Reverse to downtrend
                trend_up = False
                sar[i] = prev_ep  # SAR jumps to prior EP
                ep = low[i]
                af = acc  # reset AF
            else:
                # Continue uptrend
                sar[i] = curr_sar
                if not np.isnan(high[i]) and high[i] > prev_ep: # Check current high for NaN
                    ep = high[i]
                    af = min(prev_af + acc, max_af)
                # else: ep and af remain prev_ep, prev_af - this is implicit
        else: # downtrend
            if not np.isnan(high[i]) and high[i] > curr_sar: # Check current high for NaN
                # Reverse to uptrend
                trend_up = True
                sar[i] = prev_ep  # SAR jumps to prior EP
                ep = high[i]
                af = acc  # reset AF
            else:
                # Continue downtrend
                sar[i] = curr_sar
                if not np.isnan(low[i]) and low[i] < prev_ep: # Check current low for NaN
                    ep = low[i]
                    af = min(prev_af + acc, max_af)
                # else: ep and af remain prev_ep, prev_af - this is implicit

    return pd.Series(sar, index=df.index, name=key[0])


def _compute_fastk(df: pd.DataFrame, input_dict: dict):
    """
    Returns raw %K Series for Slow Stoch, based on continuous data.
    """
    # Ensure columns are uppercase and numeric
    df_processed = df.copy()
    df_processed.columns = df_processed.columns.str.upper()

    required_cols = ["HIGH", "LOW", "CLOSE"]
    for col in required_cols:
        if col not in df_processed.columns:
            print(f"Warning: Missing required column '{col}' for StochF calculation. Returning all-NaN series.")
            return pd.Series(np.nan, index=df.index), None # Return None for grp
        df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').ffill().bfill()
        
    h = df_processed["HIGH"]
    l = df_processed["LOW"]
    c = df_processed["CLOSE"]
    
    FK = input_dict["fastkperiod"]
    
    # --- CHANGE START: REMOVE GROUPING ---
    # No grouping is applied. Rolling min/max will be over the entire series.
    hh = (
        h.rolling(FK, min_periods=FK)
         .max()
    )
    ll = (
        l.rolling(FK, min_periods=FK)
         .min()
    )
    # --- CHANGE END ---

    # Handle division by zero or NaN values
    denominator = (hh - ll)
    raw_fastk_raw = 100 * (c - ll) / denominator
    
    # Replace infinite values (from division by zero) with NaN
    raw_fastk = raw_fastk_raw.replace([np.inf, -np.inf], np.nan)
    
    # Fill remaining NaNs. Common for oscillators like Stoch to fill with 0 or a last valid value.
    # Be aware that this fills *all* NaNs, including those from insufficient data.
    raw_fastk = raw_fastk.fillna(0.0)

    # grp is no longer meaningful in this context
    grp = None 

    return raw_fastk, grp


def local_STOCHF_FASTK(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Returns raw %K for Fast Stochastic.
    """
    raw_fastk, grp = _compute_fastk(df, input_dict) # grp will be None here
    return pd.DataFrame({ key[0]: raw_fastk }, index=df.index)


def local_STOCHF_FASTD(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Takes raw %K from the same helper, then applies the second smoothing.
    """
    raw_fastk, grp = _compute_fastk(df, input_dict) # grp will be None here
    FD = input_dict["fastdperiod"]
    ma_fastd = intended_ma(int(input_dict.get("fastdmatype", 0)))

    # --- CHANGE START: REMOVE GROUPBY IN FASTD SMOOTHING ---
    # Apply MA directly to raw_fastk Series, as there are no longer groups
    fastd = ma_fastd(raw_fastk.to_frame("fastk"), {"series_type": "fastk", "time_period": FD}, ["fastd"])
    # --- CHANGE END ---

    # if result is a DataFrame, extract its first column
    if isinstance(fastd, pd.DataFrame):
        fastd = fastd.iloc[:, 0]
    
    # Fill any NaNs that might have been introduced during smoothing
    fastd = fastd.fillna(0.0) # Common for oscillators

    return pd.DataFrame({ key[0]: fastd }, index=df.index)

# ─── BBANDS ──────────────────────────────────────────────────────────────────
def _bbands_base(df: pd.DataFrame, input_dict: dict):
    """
    Returns (middle_band, std_dev) Series for BBANDS.
    """
    s       = df[input_dict["series_type"]]
    N       = input_dict["time_period"]
    ma_code = int(input_dict["matype"])
    ma_fn   = intended_ma(ma_code)

    # compute middle band
    temp = s.to_frame(name="__bb_s__")
    tpl  = {**input_dict, "series_type": "__bb_s__", "time_period": N}
    mid = _extract_first(ma_fn(temp, tpl, ["mid"]))

    # compute rolling std
    std = s.rolling(window=N, min_periods=N).std()
    return mid, std


def local_BBANDS_RMB(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Real Middle Band only.
    """
    mid, _ = _bbands_base(df, input_dict)
    return pd.DataFrame({ key[0]: mid }, index=df.index)


def local_BBANDS_RUB(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Real Upper Band = middle + nbdevup * std
    """
    nb_up = input_dict["nbdevup"]
    mid, std = _bbands_base(df, input_dict)
    upper = mid + nb_up * std
    return pd.DataFrame({ key[0]: upper }, index=df.index)


def local_BBANDS_RLB(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Real Lower Band = middle - nbdevdn * std
    """
    nb_dn = input_dict["nbdevdn"]
    mid, std = _bbands_base(df, input_dict)
    lower = mid - nb_dn * std
    return pd.DataFrame({ key[0]: lower }, index=df.index)


def local_CMO(df: pd.DataFrame, input_dict: dict, key: list):

    series = df[input_dict["series_type"]]
    N = input_dict["time_period"]
    delta = series.diff()

    gains = delta.where(delta > 0, 0).rolling(N, min_periods=N).sum()
    losses = -delta.where(delta < 0, 0).rolling(N, min_periods=N).sum()

    cmo = 100 * (gains - losses) / (gains + losses)
    return cmo.rename(key[0])

def _aroon_base(df: pd.DataFrame, input_dict: dict):
    """
    Compute both Aroon Up and Aroon Down as pandas Series.
    Returns (aroon_up, aroon_down), both length‑df.index.
    """
    high = df["HIGH"]
    low  = df["LOW"]
    N    = int(input_dict["time_period"])

    rolled_high = high.rolling(N, min_periods=N)
    rolled_low  = low.rolling(N, min_periods=N)

    def periods_since_max(x):
        return len(x) - 1 - np.argmax(x)
    def periods_since_min(x):
        return len(x) - 1 - np.argmin(x)

    ps_high = rolled_high.apply(periods_since_max, raw=True)
    ps_low  = rolled_low.apply(periods_since_min, raw=True)

    aroon_up   = (N - ps_high) / N * 100
    aroon_down = (N - ps_low)  / N * 100

    return aroon_up, aroon_down


def local_AROON_AU(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Aroon Up only.
    key[0] should be the column name for Aroon Up.
    """
    aroon_up, _ = _aroon_base(df, input_dict)
    return pd.DataFrame({ key[0]: aroon_up }, index=df.index)


def local_AROON_AD(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Aroon Down only.
    key[0] should be the column name for Aroon Down.
    """
    _, aroon_down = _aroon_base(df, input_dict)
    return pd.DataFrame({ key[0]: aroon_down }, index=df.index)

def local_AROONOSC(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Aroon Oscillator = AroonUp − AroonDown
    """
    aroon_up = local_AROON_AU(df, input_dict, ["AROON_AU"])[
        "AROON_AU"
    ]  # extract Series
    aroon_down = local_AROON_AD(df, input_dict, ["AROON_AD"])[
        "AROON_AD"
    ]  # extract Series
    osc = aroon_up - aroon_down
    return pd.DataFrame({key[0]: osc}, index=df.index)

def local_ULTOSC(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Ultimate Oscillator (UO):
      BP_t = Close_t - min(Low_t, Close_{t-1})
      TR_t = max(
                High_t - Low_t,
                |High_t  - Close_{t-1}|,
                |Low_t   - Close_{t-1}|
              )
      avg_i = sum(BP_{t-n_i+1..t}) / sum(TR_{t-n_i+1..t})
      UO_t   = 100 * (4*avg1 + 2*avg2 + avg3) / 7
    """
    # periods
    n1 = input_dict["timeperiod1"]
    n2 = input_dict["timeperiod2"]
    n3 = input_dict["timeperiod3"]

    high       = df["HIGH"]
    low        = df["LOW"]
    close      = df["CLOSE"]
    prev_close = close.shift(1)

    # 1) Buying Pressure
    bp = close - pd.concat([low, prev_close], axis=1).min(axis=1)

    # 2) True Range
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low  - prev_close).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # 3) Rolling sums for each window
    sum_bp1 = bp.rolling(window=n1, min_periods=n1).sum()
    sum_tr1 = tr.rolling(window=n1, min_periods=n1).sum()
    sum_bp2 = bp.rolling(window=n2, min_periods=n2).sum()
    sum_tr2 = tr.rolling(window=n2, min_periods=n2).sum()
    sum_bp3 = bp.rolling(window=n3, min_periods=n3).sum()
    sum_tr3 = tr.rolling(window=n3, min_periods=n3).sum()

    # 4) Averages
    avg1 = sum_bp1 / sum_tr1
    avg2 = sum_bp2 / sum_tr2
    avg3 = sum_bp3 / sum_tr3

    # 5) Ultimate Oscillator
    uo = 100 * (4*avg1 + 2*avg2 + avg3) / 7

    return uo.rename(key[0])

def local_PLUS_DM(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Raw Plus Directional Movement (+DM):
      +DM_t = max(High_t - High_{t-1}, 0) if that > (Low_{t-1}-Low_t), else 0.
    """
    plus_dm, _, _ = _directional_movements(df)
    return plus_dm.rename(key[0])


def local_AD(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Chaikin Accumulation/Distribution Line (A/D):
      1) Money Flow Multiplier = ((Close − Low) − (High − Close)) / (High − Low)
      2) Money Flow Volume     = Multiplier × Volume
      3) A/D Line              = cumulative sum of Money Flow Volume
    """
    high  = df["HIGH"]
    low   = df["LOW"]
    close = df["CLOSE"]
    vol   = df["VOLUME"]

    # avoid division by zero
    denom = (high - low).replace(0, np.nan)
    mfm   = ((close - low) - (high - close)) / denom
    mfv   = mfm * vol
    ad    = mfv.cumsum().ffill().fillna(0)

    return ad.rename(key[0])


def local_ADOSC(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Chaikin A/D Oscillator:
      ADOSC = EMA_fast( A/D Line ) − EMA_slow( A/D Line )
    where EMAs use periods fastperiod and slowperiod.
    """
    fast = input_dict["fastperiod"]
    slow = input_dict["slowperiod"]

    # get the A/D line
    ad_line = local_AD(df, input_dict, [None])

    ema_fast = ad_line.ewm(span=fast, adjust=False).mean()
    ema_slow = ad_line.ewm(span=slow, adjust=False).mean()

    adosc = ema_fast - ema_slow
    return adosc.rename(key[0])


def local_OBV(df: pd.DataFrame, input_dict: dict, key: list):
    """
    On-Balance Volume (OBV):
      OBV_t = OBV_{t-1} + Volume_t if Close_t > Close_{t-1}
             OBV_{t-1} − Volume_t if Close_t < Close_{t-1}
             OBV_{t-1}           if equal
    """
    close = df["CLOSE"]
    vol   = df["VOLUME"]

    prev_close = close.shift(1)
    direction  = np.sign(close - prev_close).fillna(0)

    obv = (direction * vol).cumsum()
    return obv.rename(key[0])


def local_MINUS_DM(df: pd.DataFrame, input_dict: dict, key: list):
    """
    Raw Minus Directional Movement (−DM):
      −DM_t = max(Low_{t-1} - Low_t, 0) if that > (High_t - High_{t-1}), else 0.
    """
    _, minus_dm, _ = _directional_movements(df)
    return minus_dm.rename(key[0])


def local_HT_TRENDLINE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:
    """
    Hilbert Transform Trendline:
      1) Compute the analytic signal (Hilbert) of the chosen series
      2) Extract its real part (in‐phase)
      3) Smooth with a 10‐period MA

    This version:
      • Lowercases the requested series_type
      • Forward‐fills and back‐fills any NaNs before Hilbert
      • Returns all‐NaN if there’s no real data left
    """
    # 1) Normalize series_type → lowercase
    col = input_dict["series_type"]
    if col not in df.columns:
        # Column doesn’t exist at all → return all‐NaN series
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    # 2) Grab raw price series and force float dtype
    price_s = df[col].astype(float)

    # 3) Forward‐fill then back‐fill so no NaNs remain (if you prefer dropping initial points,
    #    you could drop them instead of bfill; but most Hilbert implementations expect no NaNs.)
    price_filled = price_s.ffill().bfill()

    # 4) If that still ended up all NaNs (e.g. df was empty or never had any quote), bail
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    # 5) Compute analytic signal → in_phase component
    analytic = hilbert(arr)
    in_phase = np.real(analytic)

    # 6) Smooth with a 10‐period SMA
    trend = (
        pd.Series(in_phase, index=df.index, name=key[0])
          .rolling(window=10, min_periods=1)
          .mean()
    )
    if trend.isnull().all():
        print("local_HT_TRENDLIOEN: null")
    return trend

def local_HT_SINE_SINE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:
    """
    Hilbert Transform Sine (no lead), computed from instantaneous phase.
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    analytic = hilbert(arr)
    inst_phase = np.unwrap(np.angle(analytic))
    sine_vals = np.sin(inst_phase)
    return pd.Series(sine_vals, index=df.index, name=key[0])


def local_HT_SINE_LEAD_SINE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:
    """
    Hilbert Transform Lead Sine (shifted π/4), computed from instantaneous phase.
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    analytic = hilbert(arr)
    inst_phase = np.unwrap(np.angle(analytic))
    lead_sine_vals = np.sin(inst_phase + np.pi / 4)
    return pd.Series(lead_sine_vals, index=df.index, name=key[0])


def local_HT_DCPERIOD(df: pd.DataFrame, input_dict: dict, key: list) -> pd.DataFrame:
    """
    Dominant Cycle Period:
        1) Compute analytic signal
        2) Compute instantaneous phase and its delta
        3) period = 2π / Δphase
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    analytic = hilbert(arr)
    inst_phase = np.unwrap(np.angle(analytic))
    delta_phase = np.diff(inst_phase, prepend=inst_phase[0])
    period = 2 * np.pi / np.where(delta_phase != 0, delta_phase, np.nan)
    return pd.DataFrame({key[0]: period}, index=df.index)


def local_HT_PHASOR_PHASE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.DataFrame:
    """
    In-Phase component of the Hilbert Transform (real part).
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    analytic = hilbert(arr)
    in_phase = np.real(analytic)
    return pd.DataFrame({key[0]: in_phase}, index=df.index)


def local_HT_PHASOR_QUADRATURE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.DataFrame:
    """
    Quadrature component of the Hilbert Transform (imaginary part).
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.DataFrame({key[0]: [np.nan] * len(df)}, index=df.index)

    analytic = hilbert(arr)
    quad = np.imag(analytic)
    return pd.DataFrame({key[0]: quad}, index=df.index)


def local_HT_TRENDMODE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:
    """
    TrendMode: 1 if |in_phase| > |quad|, else 0.
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    analytic = hilbert(arr)
    in_phase = np.real(analytic)
    quad     = np.imag(analytic)
    mode = (np.abs(in_phase) > np.abs(quad)).astype(int)
    return pd.Series(mode, index=df.index, name=key[0])


def local_HT_DCPHASE(df: pd.DataFrame, input_dict: dict, key: list) -> pd.Series:
    """
    Dominant Cycle Phase (unwrapped phase).
    """
    col = input_dict["series_type"]
    if col not in df.columns:
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    price_filled = df[col].astype(float).ffill().bfill()
    arr = price_filled.to_numpy()
    if arr.size == 0 or np.all(np.isnan(arr)):
        return pd.Series([np.nan] * len(df), index=df.index, name=key[0])

    analytic = hilbert(arr)
    phase = np.unwrap(np.angle(analytic))
    return pd.Series(phase, index=df.index, name=key[0])

def local_PREVCLOSE(df, input_dict, key):
    series = df["CLOSE"]
    prev = series.ffill().shift(1)
    prev.name = key[0]
    return prev

def local_CHANGE(df, input_dict, key):
    series = df["CLOSE"]
    prev = series.ffill().shift(1)
    change = series - prev
    change.name = key[0]
    return change

def local_PCTCHANGE(df, input_dict, key):
    series = df["CLOSE"]
    prev = series.ffill().shift(1).replace(0, np.nan)
    pct = (series - prev) / prev
    pct.name = key[0]
    return pct

LOCAL_FUNCS = {
    "OPEN": local_OPEN,
    "HIGH": local_HIGH,
    "LOW": local_LOW,
    "CLOSE": local_CLOSE,
    "VOLUME": local_VOLUME,

    'PREVCLOSE':    local_PREVCLOSE,
    'CHANGE':       local_CHANGE,
    'PCTCHANGE':    local_PCTCHANGE,
    "SMA":                 local_SMA,
    "EMA":                 local_EMA,
    "WMA":                 local_WMA,
    "DEMA":                local_DEMA,
    "TEMA":                local_TEMA,
    "TRIMA":               local_TRIMA,
    "FAMA":                local_FAMA,
    "MAMA":                local_MAMA,
    "VWAP":                local_VWAP,
    "T3":                  local_T3,

    "MACD_MACD":           local_MACD_MACD,
    "MACD_SIGNAL":         local_MACD_SIGNAL,
    "MACD_HIST":           local_MACD_HIST,

    "MACDEXT_MACD":        local_MACDEXT_MACD,
    "MACDEXT_SIGNAL":      local_MACDEXT_SIGNAL,
    "MACDEXT_HIST":        local_MACDEXT_HIST,

    "STOCH_SLOWK":         local_STOCH_SLOWK,
    "STOCH_SLOWD":         local_STOCH_SLOWD,

    "STOCHF_FASTK":        local_STOCHF_FASTK,
    "STOCHF_FASTD":        local_STOCHF_FASTD,

    "RSI":                 local_RSI,

    "STOCHRSI_FASTK":      local_STOCHRSI_FASTK,
    "STOCHRSI_FASTD":      local_STOCHRSI_FASTD,

    "WILLR":               local_WILLR,

    "ADX":                 local_ADX,
    "ADXR":                local_ADXR,
    "APO":                 local_APO,
    "PPO":                 local_PPO,
    "MOM":                 local_MOM,
    "BOP":                 local_BOP,
    "CCI":                 local_CCI,
    "CMO":                 local_CMO,

    "ROC":                 local_ROC,
    "ROCR":                local_ROCR,

    "AROON_AD":            local_AROON_AD,
    "AROON_AU":            local_AROON_AU,
    "AROONOSC":            local_AROONOSC,



    "MFI":                 local_MFI,

    "TRIX":                local_TRIX,

    "ULTOSC":              local_ULTOSC,
    "DX":                  local_DX,
    "MINUS_DI":            local_MINUS_DI,
    "PLUS_DI":             local_PLUS_DI,
    "MINUS_DM":            local_MINUS_DM,
    "PLUS_DM":             local_PLUS_DM,

    "BBANDS_RUB":          local_BBANDS_RUB,
    "BBANDS_RMB":          local_BBANDS_RMB,
    "BBANDS_RLB":          local_BBANDS_RLB,

    "MIDPOINT":            local_MIDPOINT,
    "MIDPRICE":            local_MIDPRICE,

    "SAR":                 local_SAR,
    "TRANGE":              local_TRANGE,
    "ATR":                 local_ATR,
    "NATR":                local_NATR,

    "AD":                  local_AD,
    "ADOSC":               local_ADOSC,
    "OBV":                 local_OBV,

    "HT_TRENDLINE":        local_HT_TRENDLINE,

    "HT_SINE_SINE":        local_HT_SINE_SINE,
    "HT_SINE_LEAD_SINE":   local_HT_SINE_LEAD_SINE,

    "HT_TRENDMODE":        local_HT_TRENDMODE,

    "HT_DCPERIOD":         local_HT_DCPERIOD,
    "HT_DCPHASE":          local_HT_DCPHASE,
    "HT_PHASOR_PHASE":     local_HT_PHASOR_PHASE,
    "HT_PHASOR_QUADRATURE":local_HT_PHASOR_QUADRATURE,
}

AGAUSSIAN = [
    "OPEN",
    "HIGH",
    "LOW" ,
    "CLOSE",
    "PREVCLOSE" ,
    "SMA", 
    "EMA",
    "WMA",
    "DEMA",
    "TEMA",
    "TRIMA",
    "FAMA",
    "MAMA",
    "VWAP",
    "T3",
     "BBANDS_RUB", 
     "BBANDS_RMB", 
     "BBANDS_RLB",

     "MIDPOINT",
     "MIDPRICE",

     "SAR",

     "HT_TRENDLINE",
 
     "HT_PHASOR_PHASE"

]

PERCENTILE = [


     "STOCH_SLOWK", 
     "STOCH_SLOWD",
     "STOCHF_FASTK", 
     "STOCHF_FASTD", 
     "RSI",
     "STOCHRSI_FASTK", 
     "STOCHRSI_FASTD",
     "WILLR",

      "ADX",
     "ADXR",

     "CMO",

     "AROON_AD",
     "AROON_AU",
     "AROONOSC",

     "MFI",



     "ULTOSC",
     "DX",
     "MINUS_DI",

     "PLUS_DI",






 
     "HT_TRENDLINE",
  
     "HT_PHASOR_PHASE"

]



HUGE = [
    "VOLUME"
     "AD",
     "ADOSC",
     "OBV",
     "HT_TRENDLINE",
     "HT_PHASOR_PHASE"
     "HT_DCPERIOD",
]

CONSTANT =[
        "CHANGE",
    "PCTCHANGE",
         "BOP",
              "TRANGE",
                   "ATR",
     "NATR",
          "CCI",
     "ROC",
     "ROCR",
     "PLUS_DM",
          "MINUS_DM",
     "HT_TRENDMODE",
          "HT_SINE_SINE", 
               "MOM",
     "HT_SINE_LEAD_SINE",
          "HT_PHASOR_QUADRATURE", 
               "TRIX",
                   "MACD_MACD",
    "MACD_Signal",
    "MACD_Hist",
        "MACDEXT_MACD", 
     "MACDEXT_SIGNAL",
     "MACDEXT_HIST",
          "APO",
     "PPO",
]