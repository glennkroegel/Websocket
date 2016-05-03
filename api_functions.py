'''
api_functions.py

@version: 1.0

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com
@summary: API and custon function list for websocket connection.

'''

import socket
import hashlib
import cgi
import threading
import time
import json
import threading
import pika
import ast
from StringIO import StringIO
import sys

#########################################################################################################################

api_token = '76O9xxxx'

#########################################################################################################################

# AUTHORIZED CALLS (No Impact)

def authorize(ws):

	data = {'authorize': api_token}
	message = json.dumps(data)
	ws.send(message)

def statement():

	data = {'statement': 1}
	message = json.dumps(data)

def balance(ws):

	data = {'balance': 1}
	message = json.dumps(data)
	ws.send(message)

def account_status():

	data = {'get_account_status': 1}
	message = json.dumps(data)

#########################################################################################################################

# UNAUTHENTICATED CALLS

def tick_history(ws, asset, end = "latest", count = 30, style = "candles", granularity = 60, req_id = 1):

	request = 	{
					"ticks_history": asset,
					"end": end,
					"count": count,
					"style": style,
					"granularity": granularity,
					"req_id": req_id
				}

	message = json.dumps(request)
	ws.send(message)

def server_time(ws):

	data = {'time': 1}
	message = json.dumps(data)
	ws.send(message)

#########################################################################################################################

# UNAUTHENTICATED STREAMS

def tick_stream(ws, asset = 'R_50'):

	message = json.dumps({'ticks': asset})
	ws.send(message)

def price_proposal(ws, data):

	message = json.dumps(data)
	ws.send(message)

#########################################################################################################################

# MISC FUNCTION LIST ------->

#########################################################################################################################

# MODEL FEATURES

def calcFeaturesLocally(df, asset = 'frxEURUSD'):

	df = copy.deepcopy(df)
	df = np.round(df, decimals = 8)

	if asset == 'frxEURUSD':
		df['RSI'] = taCalcIndicator(df, 'RSI', window = 30)
		df['WILLR'] = taCalcIndicator(df, 'WILLR', window = 30)
		df['WILLR_M1'] = df['WILLR'].pct_change()
		df['WILLR_M2'] = df['WILLR'].pct_change(2)
		df['WILLR_M5'] = df['WILLR'].pct_change(5)
		df['WILLR_M10'] = df['WILLR'].pct_change(10)
		return df
	else:
		logging.info('No features defined for asset')
		pass

#########################################################################################################################

# TRADE ACTIONS

def tradeActions(asset, px, dt_last_bar, passthrough):

	px = float(px)
	dt_last_bar = str(dt_last_bar)

	offset = 60
	delta = 300 + offset
	dt_last_bar = dt.datetime.strptime(dt_last_bar, '%Y-%m-%d %H:%M:%S')
	t = dt_last_bar + dt.timedelta(seconds = delta) - dt.datetime(1970,1,1)
	t = int(t.total_seconds())

	proposal = 	{
			        "proposal": 1,
			        "amount": "10",
			        "basis": "stake",
			        "contract_type": "CALL",
			        "currency": "USD",
			        "date_expiry": t,
			        "symbol": asset,
			        "passthrough": {"last_close": passthrough}
				}

	if asset == 'frxEURUSD':

		if (px > 0.6):
			proposal['contract_type'] = "CALL"
			return proposal
		elif (px < 0.4):
			proposal['contract_type'] = "PUT"
			return proposal
		else:
			return None
	else:
		logging.info('No trade actions defined for asset')
		return None


def marketConditions(data, asset):

	if asset == 'frxEURUSD':

		c1 = True 
		c2 = (time.gmtime().tm_hour >= 9) & (time.gmtime().tm_hour < 18)

		if (c1 == True) & (c2 == True):
			return True
		else:
			return False
	else:
		logging.info('No market condition for asset')
		return False

#########################################################################################################################

# VALIDITY CHECKS

def dataCheck(ls_x):

	c1 = np.isnan(ls_x).any()
	c2 = np.isinf(ls_x).any()

	if (c1 == True) or (c2 == True):
		logging.info('Data check failed')
		return False
	else:
		return True

def timeCheck(dt_time, delta = 1):

	# FIX

	dt_now = pd.to_datetime(time.time(), unit = 's')
	dt_last_bar = dt_time

	c1 = True
	c2 = True

	if (c1 == True) & (c2 == True):
		return True
	else:
		logging.info('Time check failed')
		return False

def timeCheck2(dt_now, dt_proposal, max_delay = 20):

	#dt_now = pd.to_datetime(time.time(), unit = 's') # investigate date_start on proposal response
	dt_now = pd.to_datetime(int(dt_now), unit = 's')
	dt_proposal = pd.to_datetime(int(dt_proposal), unit = 's')
	delay = dt_now - dt_proposal

	c1 = delay.seconds < max_delay 
	c2 = True

	if (c1 == True) & (c2 == True):
		return True
	else:
		logging.info('Time check (2) failed')
		return False

def payoutCheck(ask_price, payout_price, min_payout = 0.63):

	# ask_price - contract stake / trade amount
	# payout_price - return value of contract : ask_price + profit

	ask_price = float(ask_price)
	payout_price = float(payout_price)

	payout = (payout_price - ask_price)/ask_price # placeholder - FIX

	if (payout >= min_payout):
		return True
	else:
		logging.info('Payout too low: {0}'.format(payout))
		return False

def spotCheck(close_price, spot_price, asset):

	# close_price - close price from last bar used in model calculation
	# spot_price - current spot price for contract

	
	if asset == 'frxEURUSD':
		error_threshold = 0.00005
	else:
		logging.info('No spot check for asset')
		error_threshold = 0.0001

	delta = np.absolute(float(close_price) - float(spot_price))

	if (delta < error_threshold):
		return True
	else:
		logging.info('Price moved')
		return False

#########################################################################################################################

# FUNCTIONS

def formatBars(data):

	# Receive JSON bars - Format bars into dataframe with correct time index

	df_bars = pd.DataFrame(data)

	df_bars = df_bars.rename(columns = {'epoch': 'DATETIME', 'open': 'OPEN', 'high': 'HIGH', 'low': 'LOW', 'close': 'CLOSE'})
	df_bars['DATETIME'] = pd.to_datetime(df_bars['DATETIME'], unit = 's')
	df_bars = df_bars.set_index('DATETIME')
	df_bars['VOLUME'] = np.zeros(df_bars['CLOSE'].shape)

	df_bars = pd.DataFrame(df_bars, dtype = 'float')
	df_bars = df_bars.head(len(df_bars)-1)

	return df_bars

def onBar(data, time_delta = 60):

	try:
		dt_now = dt.datetime.utcfromtimestamp(int(data['req_id']))
		dt_current_bar = dt.datetime.utcfromtimestamp(int(data['candles'][-1]['epoch']))
		dt_last_bar = dt.datetime.utcfromtimestamp(int(data['candles'][-2]['epoch']))

		c1 = dt_now == dt_current_bar # ensuring that bar is complete by checking that there is a new incomplete bar
		c2 = (dt_now - dt_last_bar).seconds == time_delta

		if (c1 == True) & (c2 == True):
			return True
		else:
			return False
	except:
		logging.info('OnBar error')
		return False

def getAmount(balance, proportion):

	amount = np.round(proportion*balance)

	if (amount > 10000):
		return 10000
	elif (amount < 10):
		return 10
	else:
		return amount

def send(ws):

	authorize(ws)
	tick_stream(ws)

