import websocket
from datetime import datetime
import json
import requests
import signal


class CryptoCollector():
    def __init__(self, symbol_list, collector_opt = None):
        if collector_opt is None:
            collector_opt = {"coll_types": [ "orderbook", "trade" ]}
        self.collector_opt = collector_opt
        self.symbol_list = symbol_list

        self.coll_type = self.collector_opt.get("coll_types")
        if self.coll_type is None:
            raise Exception("unexpected collector_opt!! It must have coll_types in it's dict keys!")
        self.orderbook_flag = "orderbook" in self.coll_type
        self.trade_flag = "trade" in self.coll_type


        if self.orderbook_flag == False and self.trade_flag == False:
            raise Exception("unexpected collector_opt!! It must have value 'orderbook' or 'trade' in key named by 'coll_types'!")
        
        if symbol_list is None:
            self.symbol_list= self.VAILD_SYMBOL_LIST[:]
        else:
            self.symbol_list=[]
            for sym in symbol_list:
                if sym in self.VAILD_SYMBOL_LIST:
                    self.symbol_list.append(sym)
        
    def _log(self, msg):
        print(msg)
    
    def get_symbol_list(self):
        return self.symbol_list
    
    def init_before_run(self):
        raise NotImplementedError
    

    def run(self):
        raise NotImplementedError




import websocket
import _thread
import time
from json import dumps
from kafka_objects.kafka_producer import KafkaProducerWrapper
ob_topic_name = "upbit_orderbook"
tr_topic_name = "upbit_trade"
ob_producer = KafkaProducerWrapper(
    ["34.64.98.119:9092", "34.64.145.172:9092", "34.64.252.120:9092"],
    ob_topic_name
)
tr_producer = KafkaProducerWrapper(
    ["34.64.98.119:9092", "34.64.145.172:9092", "34.64.252.120:9092"],
    tr_topic_name
)
symbol_list = ["ETH"]

SYMBOL2CODE = { s: "KRW-" + s for s in symbol_list}

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print("Error: %s" % error)

def on_close(ws, close_status_code, close_msg):
    print("### closed ###")

def on_open(ws):
    dt_string = datetime.now().strftime("%Y%m%d-%H:%M:%S")
    print(f"ws open : {dt_string}")

    sy = [{"ticket":"upbit-data-pipeline"}]
    
    d = {"type":"orderbook","codes":[]}
    for s in symbol_list:
        d["codes"].append("{}.5".format(SYMBOL2CODE[s]))
    sy.append(d)
    d = {"type":"trade","codes":[]}
    for s in symbol_list:
        d["codes"].append("{}".format(SYMBOL2CODE[s]))
    sy.append(d)            
    
    s = json.dumps(sy)
    ws.send(s)
stop_flag = False
def signal_handler(sig, frame):
    global stop_flag
    stop_flag = True
    try: 
        ws.close()
    except: 
        pass
if __name__ == "__main__":
    upbit_url = "wss://api.upbit.com/websocket/v1"
    
    signal.signal(signal.SIGINT, signal_handler)
    while not stop_flag:
        print("websocket connect : ", upbit_url, stop_flag)
        ws = websocket.WebSocketApp(upbit_url,
                                    on_open = on_open,
                                    on_message = on_message,
                                    on_error = on_error,
                                    on_close = on_close)
        try:
            ws.run_forever()
        except Exception as e:
            print('exception!!', e)
        finally:
            ws.close()
