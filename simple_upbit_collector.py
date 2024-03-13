import websocket
from datetime import datetime
import json
import requests
import numpy as np
from kafka import KafkaProducer



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
    