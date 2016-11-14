import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import smtplib

class BroadcastTrades(object):
    
    def __init__(self, **kwargs):
        
        self.__hostname = kwargs["hostname"]
        self.__portnum  = kwargs["portnum"]
        self.__username = kwargs["username"]
        self.__password = kwargs["password"]
        self.__database = kwargs["database"]
        self.__trades   = kwargs["trades"]
        self.__quotes   = kwargs["quotes"]
        
        self.__q = qconn.QConnection(host=self.__hostname, port=self.__portnum, \
                                     username=self.__username, password=self.__password, \
                                     numpy_temporals=True)
        
        self.connect()
        
    def __del__(self):
        
        self.disconnect()
    
    def connect(self):
        
        try:
            self.__q.open()
        except Exception as e:
            print(e)
    
    def disconnect(self):
        
        if self.__q.is_connected():
            self.__q.close()
     
    @property
    def tq_df(self):
        
        return self.__tq_df
    
    @property
    def sym_df(self):
        
        return self.__sym_df
    
    def get_trade_quote(self):
        
        try:
            
            self.__q.sync('\l {}'.format(self.__database))
            self.__q.sync('last_date:last date;')
            self.__q.sync('trade_tab:.st.unenum select volume:sum size,price:size wavg price by sym from {} where date=last_date,'
                          'price>1,price<1000,'
                          'sun_time within (08:30:00,15:00:00),not sym like "*ZZT",not sym like "*.TEST",not sym in `CBO`CBX;'.format(self.__trades))
            self.__q.sync('sym_lst:distinct exec sym from trade_tab;')
            self.__q.sync('quote_tab:.st.unenum select spread:med ask_price-bid_price by sym from {} where date=last_date,'
                          'sun_time within (08:30:00,15:00:00),(sym) in (sym_lst);'.format(self.__quotes))
            self.__q.sync('tq_tab:ij[trade_tab;quote_tab]')
            
            self.__last_date = self.__q.sync('last_date')
            self.__tq_df = self.__q.sync('tq_tab', pandas=True)
        
        except Exception as e:
            print(e)
        
        # Exclude outliers
        self.__tq_df = self.__tq_df[self.__tq_df.spread<self.__tq_df.spread.quantile(0.99)]
        
        return self
    
    def get_symbol_list(self, symbol_csv):
        
        try:
            self.__sym_df = pd.read_csv(symbol_csv)
        except Exception as e:
            print(e)
        
        self.__tq_df = pd.merge(self.__sym_df, self.__tq_df, left_on='symbol', right_index=True)
        
        return self
    
    def vol_avg_spread(self):
        
        return np.average(self.__tq_df.spread, weights=self.__tq_df.volume)
    
    def broadcast(self):
        
        sender = 'ygao@suntradingllc.com'
        receivers = ['ygao@suntradingllc.com', 'zluo@suntradingllc.com']
        
        message = """
        Dear all,
        
        Volume averaged spread of last trading day ({}) is
        
        {}
        
        Best wishes!
        
        Yi
        """.format(self.__last_date, self.vol_avg_spread())
         
        try:
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, message)
            #print("Successfully sent email")
        except smtplib.SMTPException:
            print("Error: cannot send email")
    
if __name__ == "__main__":
    
    kwargs = {"hostname": "kdb2", "portnum": 10102, "username": "ygao", "password": "Password23",
              "database": "/data/db_tdc_us_equities_nbbo", "trades": "trades", "quotes": "quotes"}
    
    bt = BroadcastTrades(**kwargs).get_trade_quote().get_symbol_list("symbol_list.csv").broadcast()
