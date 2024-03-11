import os
import csv
import time
import pyupbit
import datetime
import operator
import pandas as pd
import math
import numpy as np
from collections import defaultdict

os.environ['POT_BASE'] = 'D:\\'
os.environ['POT_BACKEND'] = 'pytorch'

from shalom.oil_pot_trade import settings
from shalom.oil_pot_trade import data_manager


access = "YNAJxn9de4KObEKNaUgIQjVLDRxqcnYLlvIUzxxf"
secret = "VNBnA3zQw3mxF2wPmFjAlQ5Tk1v67HmAJ1z1JoLl"

min_krw = 5050  #최소trading금액

offset_data_cnt = 10
Trading_start = False
Update_on_Time = False
load_result = False
slot_max = 10
slot_count = 0

timestamp_index = 0
timestamp_gap = 3
timestamp_trading = False

emergency_gap = 0.05

idx = []
position = defaultdict(int)

Debug_mode = False
init_operation = True


def get_ticker():
    tickers = pyupbit.get_tickers(fiat='KRW')
    return tickers


def get_sorted_tickers():
    tickers = pyupbit.get_tickers(fiat='KRW')
    values = []
    for ticker in tickers:
        day = pyupbit.get_ohlcv(ticker, count=2)
        if day is not None:
            value = day['value'][-2]
            values.append(value)
        time.sleep(0.1)

    sorted_tickers = [x for _, x in sorted(zip(values, tickers), reverse=True)]
    return sorted_tickers


def get_balance(ticker): #without KRW
    # """잔고 조회"""
    basic_name = ticker
    if ticker is not "KRW":
        basic_name = ticker[4:]
    balances = upbit.get_balances()
    for b in balances:
        if b['currency'] == basic_name:
            if b['balance'] is not None:
                return float(b['balance'])
    return 0.0

def get_buy_price(ticker):  #without KRW
    # """매수가격 조회"""
    basic_name = ticker[4:]
    balances = upbit.get_balances()
    for b in balances:
        if b['currency'] == basic_name:
            if b['avg_buy_price'] is not None:
                return float(b['avg_buy_price'])
    return 0.0

def get_cur_price(ticker):
    # """현재가격 조회"""
    return pyupbit.get_current_price(ticker)


def get_holding_count():
    count = 0
    for coin_name in get_ticker():
        if get_balance(coin_name) != 0.0 and get_balance(coin_name) * get_buy_price(coin_name) > min_krw:
            if Debug_mode:
                return 0
            else:
                count += 1
    return count


def get_Holdcoin(ticker, won):     # 잔량조회
    balance = get_balance(ticker)
    price = get_cur_price(ticker)
    total = price * balance
    if total - won < min_krw:
        return balance
    else: return won / price


def sell_market_order(name, balance):
    if Debug_mode:
        pass
    else:
        upbit.sell_market_order(name, balance)
    

def buy_market_order(name, price):
    if Debug_mode:
        pass
    else:
        upbit.buy_market_order(name, price * 0.9995)
    

def is_BnS_Signal(ticker):

    """
    BnS 전략에 따른 매매 시그널을 생성합니다.

    Args:
    ticker: 거래 대상 종목 코드
    hold_coin: 현재 보유하고 있는 코인 수량 (0 또는 1)

    Returns:
    0: 매수 시그널
    1: 매도 시그널
    2: 유지 (현재 상태 유지)
    """
    ohlcv = pyupbit.get_ohlcv(ticker, interval="minute60", count=100)
    df = pd.DataFrame(ohlcv)

    # 이동 평균선 계산
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['MA30_Volume'] = df['volume'].rolling(30).mean()

    df['MA12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['MA26'] = df['close'].ewm(span=26, adjust=False).mean()

    # MACD
    df['MACD_line'] = df['MA12'] - df['MA26']
    df['MACD_signal'] = df['MACD_line'].rolling(9).mean()
    df['MACD_hist'] = df['MACD_line'] - df['MACD_signal']

    # RSI
    df['delta'] = df['close'].diff(1)
    df['UP'] = np.where(df['delta']>=0, df['delta'], 0)
    df['DN'] = np.where(df['delta'] <0, df['delta'].abs(), 0)

    # welles moving average
    df['AU'] = df['UP'].ewm(alpha=1/14, min_periods=14).mean()
    df['AD'] = df['DN'].ewm(alpha=1/14, min_periods=14).mean()
    df['RS'] = df['AU'] / df['AD']
    df['RSI'] = 100 - (100 / (1 + df['RS']))
    df['RSI_signal'] = df['RSI'].rolling(9).mean()

    # 스토캐스틱
    df['Lowest14'] = df['low'].rolling(14).min()
    df['Highest14'] = df['high'].rolling(14).max()
    df['K'] = (df['close'] - df['Lowest14']) / (df['Highest14'] - df['Lowest14']) * 100
    df['D'] = df['K'].rolling(3).mean()

    hold_coin = get_balance(ticker)

    """매수 신호를 확인하는 함수"""
    # 5이평선이 20이평선 위로 교차 (골든 크로스)
    golden_cross = df['MA5'].iloc[-2] > df['MA20'].iloc[-2] and df['MA5'].iloc[-3] < df['MA20'].iloc[-3]
   # 5이평선이 20이평선 아래로 교차 (데드크로스)
    dead_cross = df['MA5'].iloc[-2] < df['MA20'].iloc[-2] and df['MA5'].iloc[-3] > df['MA20'].iloc[-3]

    # MACD
    macd_cross_up = df['MACD_hist'].iloc[-3] <= 0 and df['MACD_hist'].iloc[-2] >= 0 
    macd_cross_dn = df['MACD_hist'].iloc[-3] >= 0 and df['MACD_hist'].iloc[-2] <= 0 
    macd_dn = df['MACD_hist'].iloc[-2] <= 0 

    # RSI
    rsi_cross_up = df['RSI'].iloc[-3] < df['RSI_signal'].iloc[-3] and df['RSI'].iloc[-2] > df['RSI_signal'].iloc[-2] 
    rsi_cross_dn = df['RSI'].iloc[-3] > df['RSI_signal'].iloc[-3] and df['RSI'].iloc[-2] < df['RSI_signal'].iloc[-2]

    # 스토캐스틱
    if df['K'].iloc[-2] < 20 and df['D'].iloc[-2] < 20:
        stochastic_cross = True
    elif df['K'].iloc[-2] > 80 and df['D'].iloc[-2] > 80:
        stochastic_cross = False

    # RSI
    rsi_above_70 = df['RSI'].iloc[-2] > 70
    rsi_above_50 = df['RSI'].iloc[-2] > 50
    rsi_under_50 = df['RSI'].iloc[-2] < 50
    rsi_under_30 = df['RSI'].iloc[-2] < 30

    # Volume
    volume_up = df['MA30_Volume'].iloc[-2] < df['volume'].iloc[-2]
    volume_dn = df['MA30_Volume'].iloc[-2] > df['volume'].iloc[-2]

    # 매매 조건
    # if (macd_cross_up and rsi_above_50 and hold_coin == 0.0) or (golden_cross and rsi_above_50 and hold_coin == 0.0):
    #     signal = 'BUY'
    #     if (macd_cross_up and rsi_above_50 and hold_coin == 0.0): chk = 1
    #     elif (golden_cross and rsi_above_50 and hold_coin == 0.0): chk = 2
    # elif macd_cross_dn or dead_cross :
    #     signal = 'SELL'
    #     if (macd_cross_dn): chk = 3
    #     elif dead_cross: chk = 4
    # else:
    #     signal = 'HOLD'

    # if signal == 'HOLD':
    #     return 2, chk
    # else:
    #     return 0 if signal == 'BUY' else 1 if signal == 'SELL' else 2 ,chk

    sto_ovsold = df['K'].iloc[-2] < 20 and df['D'].iloc[-2] < 20
    #if ticker == 'KRW-BTC':
    #    print(df['MACD_line'].iloc[-4],df['MACD_line'].iloc[-3], df['MACD_line'].iloc[-2],df['MACD_line'].iloc[-1])

    if macd_cross_up and rsi_above_50 and volume_up and hold_coin == 0.0:
        signal = 'BUY'
    elif (macd_cross_dn and rsi_under_50) or (macd_dn and rsi_under_50):
        signal = 'SELL'
    else:
        signal = 'HOLD'

    return signal, int(macd_cross_up), int(macd_cross_dn), int(macd_dn), int(rsi_above_50), int(rsi_under_50), int(volume_up),  int(volume_dn)


# 로그인
upbit = pyupbit.Upbit(access, secret)
print("autotrade start")

idx = get_ticker()
#idx = get_sorted_tickers()
all_tk = len(idx)


# 자동매매 시작
while True:
    try:
        now = datetime.datetime.now()

        if now.minute == 00 or timestamp_trading == True:
            if timestamp_trading == False:
                available_amount = get_balance('KRW')
                usage_amount = upbit.get_amount('ALL')
                unit_amount = (available_amount + usage_amount) / all_tk
                slot_count = 0
                print()
                print("BALANCE - - - ava:",available_amount,"used:",usage_amount,"unit_amount", unit_amount)
                print()

            timestamp_trading = True
            in_price = 0
            out_price = 0

            coin_name = idx[timestamp_index]
            HoldCoin = get_balance(coin_name)
            cur_price = get_cur_price(coin_name)
            buy_price = get_buy_price(coin_name)            
            detect, A,B,C,D,E,F,G = is_BnS_Signal(coin_name)
            if HoldCoin != 0.0: slot_count += 1
            
            in_price = int (buy_price * HoldCoin)
            out_price = int(cur_price * HoldCoin)

            dashes = '-' * 80
            print(dashes)

            f_string = f"""{timestamp_index} {coin_name}\t -{detect}- Macd :{A:.0f}:{B:.0f}:{C:.0f}: RSI {D:.0f}:{E:.0f}, Vol {F:.0f}:{G:.0f} ::  {in_price} --> {out_price} \t---{now.hour}:{now.minute}:{now.second}"""

            if detect == 'BUY': #buy
                buy_market_order(coin_name, unit_amount)
                slot_count += 1

                print(f_string)
            elif detect ==  'SELL': #sell
                if HoldCoin != 0.0:
                    sell_market_order(coin_name, HoldCoin)
                    slot_count -= 1

                    print(f_string)
                else:
                    detect = 'SELL'
                    print(f_string)
            else:
                print(f_string)

            timestamp_index += 1
            if timestamp_index >= len(idx): 
                timestamp_index = 0
                timestamp_trading = False

                print("HOLDING COUNT:::", slot_count)
                print()
            time.sleep(0.5)
        else:
            #print("--- Wait trading ---",now.hour,":",now.minute,":",now.second)
            print(f'\r --- Wait trading ---: {now.hour}:{now.minute}:{now.second}', end='')
            time.sleep(1)

    except Exception as e:
        print(e)
        time.sleep(5)