'''
Websocket.py

@version: 1.0

Real time execution using trained model and websocket

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com
@summary: 

'''

import socket
import hashlib
import cgi
import threading
import time
import json
import threading
import pika
import datetime as dt
import ast
import websocket
import pandas as pd
import numpy as np
import ssl
from StringIO import StringIO
from api_functions import *
import sys
import logging
from sklearn.externals import joblib

# CUSTOM IMPORTS

from Functions import *
from Technical import *

#########################################################################################################################

model = joblib.load('model.pkl') # Load model
clients = []

#########################################################################################################################

# WEBSOCKET 

def on_open(ws):

	print("Server connected")
	logging.info("Server connected")
	send(ws)

def on_message(ws, message):

	res = json.loads(message.decode('utf8'))
	msg_type = res['msg_type']
	asset = 'frxAsset'

	#print("Message received: {0}".format(res))

	# CASES

	if (msg_type == 'authorize'):

		global start_balance
		global current_balance
		global trade_proportion
		global trade_x

		try:
			start_balance = float(res['authorize']['balance'])
			current_balance = start_balance
			trade_proportion = 0.00001
			trade_x = getAmount(current_balance, trade_proportion)
			print("Session authorized")
			logging.info("Authorization successful")
		except:
			logging.info("Authorization failed")
			ws.close()

	elif (msg_type =='candles'):

		if onBar(res) == True:
			
			order = None # Defailt action is to do nothing
			df_bars = formatBars(res['candles']) 

			# FEATURE CALCULATION

			df_features = calcFeaturesLocally(df_bars, asset = asset) # Function to calculate model features based on received data
			ls_exlude = ['OPEN','HIGH','LOW','CLOSE','VOLUME'] # columns not required for model input
			cols = [col for col in df_features.columns if col not in ls_exlude]
			df_x = df_features[cols] # filter to obtain only necessary cols

			# PROBABILITY CALCULATION

			if marketConditions(df_x, asset = asset) == True:

				ls_x = df_x.iloc[-1:].as_matrix()

				if (dataCheck(ls_x) == False) or (timeCheck(ls_x) == False): # Check data is as expected 
					lr_px = np.array([(0.5,0.5)])
					str_px = str(lr_px[:,1][0])
					str_date = str(df_x.ix[-1:].index.format()[0])
					print("{0}: {1}".format(str_date, str_px))
				else:
					try:
						lr_px = model.predict_proba(ls_x) # Calculate outcome probabilities
						str_px = str(lr_px[:,1][0])
						str_date = str(df_x.ix[-1:].index.format()[0])
						str_close = str(df_features['CLOSE'].ix[-1:][0])
						print("{0}: {1}, Close: {2}".format(str_date, str_px, str_close))
						logging.info("{0}: {1}, Close: {2}".format(str_date, str_px, str_close))
						order = tradeActions(asset, str_px, dt_last_bar = str_date, passthrough = str_close) # Determine actions based on model result
					except Exception,e:
						lr_px = np.array([(0.5,0.5)])
						order = None
						print e
						logging.info('Model prediction failed: {0}'.format(e))
			else:
				lr_px = np.array([(0.5,0.5)])
				str_px = str(lr_px[:,1][0])
				str_date = str(df_x.ix[-1:].index.format()[0])
				print("{0}: {1}".format(str_date, str_px))
				logging.info("{0}: {1}".format(str_date, str_px))

			# SEND ORDER

			if order is not None:
				try:
					message = json.dumps(order)
					ws.send(message)
				except:
					print("Order failed to send")
					logging.info('Order failure - {0} - {1}'.format(str_date, str_px))
			else:
				pass
		else:
			#logging.info('Candle unavailable. Retrying...')
			try:
				time.sleep(0.4)
				tick_history(ws, asset = asset, count = 120, req_id = res['req_id'])
			except:
				logging.info('Retry failed')
	elif (msg_type == 'proposal'):
		try:
			proposal_id = res['proposal']['id']
			proposal_asset = res['echo_req']['symbol']		
			proposal_payout = res['proposal']['payout']
			proposal_amount = res['proposal']['ask_price']
			proposal_spot = res['proposal']['spot']
			proposal_start_time = res['proposal']['date_start']			
			proposal_spot_time = res['proposal']['spot_time']			
			passthrough_close = res['echo_req']['passthrough']['last_close'] # from passthrough
			trade_amount = trade_x

			if (payoutCheck(proposal_amount, proposal_payout) == True) and (spotCheck(passthrough_close, proposal_spot, asset = proposal_asset) == True) and (timeCheck2(proposal_start_time, proposal_spot_time) == True): # Safety flags to ensure actions consistent with model interpretation
				message = json.dumps({'buy': proposal_id, 'price': trade_amount, 'passthrough': {'entry_price': proposal_spot}})
				ws.send(message)
			else:
				logging.info('Trade skipped')
		except:
			proposal_error = res['error']['code']
			logging.info('Proposal response error: {0}'.format(proposal_error))
	elif (msg_type == 'buy'):
		try:
			purchase_time = dt.datetime.utcfromtimestamp(int(res['buy']['purchase_time'])).strftime('%Y-%m-%d %H:%M:%S')   
			purchase_shortcode = res['buy']['shortcode']
			entry_price = res['echo_req']['passthrough']['entry_price']
			logging.info('{0}: {1}: {2}'.format(purchase_time, purchase_shortcode, entry_price))
			print('{0}: {1}: {2}'.format(purchase_time, purchase_shortcode, entry_price))
		except:
			buy_error = res['error']['code']
			logging.info('Buy response error: {0}'.format(buy_error))
	elif (msg_type == 'tick'):
		epoch_tick = res['tick']['epoch']
		dt_tick = dt.datetime.utcfromtimestamp(int(epoch_tick))
		if (dt_tick.second == 0):
			tick_history(ws, asset = asset, count = 120, req_id = epoch_tick)
		elif (dt_tick.second == 30):
			try:
				message = json.dumps({'balance': 1})
				ws.send(message)
			except:
				trade_x = 1
				logging.info('Failure on balance request')
		else:
			pass
	elif (msg_type == 'balance'):
		try:
			current_balance = float(res['balance']['balance'])
			trade_x = getAmount(current_balance, trade_proportion) # Adjust proportion based on current balance - checked every minute.
			print("Balance: {0} Trade: {1}".format(current_balance, trade_x))
			#logging.info("Balance: {0} Trade: {1}".format(current_balance, trade_x))
		except:
			trade_x = 1
			logging.info('Failure on balance response')
	elif (msg_type == 'time'):
		dt_server_time = dt.datetime.utcfromtimestamp(int(res['time'])).strftime('%Y-%m-%d %H:%M:%S')
		logging.info("{0}".format(dt_server_time))
	else:
		print("Message received: {0}".format(res))
		#ws.close()
		pass

def on_error(ws, error):

	print("Websocket error: {0}".format(error))

def on_close(ws):

	print("Websocket connection closed")
	logging.info("Connection closed")

def main():

	#######################################################

	# LOG FILE

	logging.basicConfig(filename = 'ASSET.log', format = "%(asctime)s; %(message)s", datefmt = "%Y-%m-%d %H:%M:%S", level = logging.DEBUG)

	#######################################################

	print('Starting websocket..')

	websocket.enableTrace(False)
	apiURL = "wss://ws.binaryws.com/websockets/v3"
	ws = websocket.WebSocketApp(apiURL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever(sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1})



if __name__ == "__main__":

  try:

    main()

  except KeyboardInterrupt:

    print('Interupted...Exiting...')

