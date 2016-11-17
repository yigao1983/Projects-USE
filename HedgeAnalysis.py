import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import matplotlib.pyplot as plt
#import smtplib
#import email.mime.multipart as multipart
#import email.mime.text as text

class HedgeAnalysis(object):
    
    def __init__(self, date_beg, date_end, initiator, type_name, **kwargs):
        
        self.__date_beg  = date_beg
        self.__date_end  = date_end
        self.__initiator = initiator
        self.__type_name = type_name
        
        self.__hostname      = kwargs["hostname"]
        self.__portnum       = kwargs["portnum"]
        self.__username      = kwargs["username"]
        self.__password      = kwargs["password"]
        self.__f2f_database  = kwargs["f2f_database"]
        self.__f2f_fifo      = kwargs["f2f_fifo"]
        self.__fut_database  = kwargs["fut_database"]
        self.__fut_book      = kwargs["fut_book"]
        
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
    def lt_df(self):
        
        return self.__lt_df
    
    def get_liq_trd(self):
        
        try:
            self.__q.sync('\l {}'.format(self.__f2f_database))
            self.__q.sync('date_beg:{};'.format(self.__date_beg))
            self.__q.sync('date_end:{};'.format(self.__date_end))
            self.__q.sync('liq_tab:.st.unenum select date:first date,sym:first sym,close_side:first close_side,'
                          'price:hedged_qty wavg close_price,hedged_qty:sum hedged_qty by date,sym '
                          'from {} where date within (date_beg,date_end),sym like "ES*",'
                          'initiator=`{},type_name=`{},open_strategy_intent=`INITIAL_INVENTORY;'.
                          format(self.__f2f_fifo, self.__initiator, self.__type_name))
            self.__q.sync('sym_lst:distinct exec sym from liq_tab;')
            
            self.__q.sync('\l {}'.format(self.__fut_database))
            self.__q.sync('trd_tab:.st.unenum select date:first date,sym:first sym,bid_price:first bid1,ask_price:first ask1 '
                          'by date,sym from {} where date within (date_beg,date_end),sym in sym_lst,'
                          'sun_time>08:30:00.000000,sun_time<15:00:00.000000;'.format(self.__fut_book))
            self.__q.sync('lt_tab:ij[liq_tab;trd_tab];')
            
            self.__lt_df = self.__q.sync('lt_tab', pandas=True)
        
        except Exception as e:
            print(e)
        
        return self
    
    def analyze(self):
        
        self.__lt_df['benchmark_price'] = self.__lt_df.apply(lambda x: x.ask_price if x.close_side==1 else x.bid_price, axis=1)
        self.__lt_df['pnl'] = self.__lt_df.apply(lambda x: (x.benchmark_price-x.price)*(2*(x.close_side==1)-1)*x.hedged_qty*50, axis=1)
        
        return self
    
    def save_df(self):
        
        self.__lt_df.to_csv('lt_df_{}_{}.csv'.format(self.__date_beg, self.__date_end))
        
        return self
    
    def plot_price(self):
        
        self.__lt_df.plot(x=self.__lt_df.index.get_level_values(0), y=['price', 'benchmark_price'])
        plt.savefig('hedge_price_{}_{}.pdf'.format(self.__date_beg, self.__date_end), bbox_inches='tight')
        
        return self
    
    def plot_pnl(self):
        
        self.__lt_df.plot(x=self.__lt_df.index.get_level_values(0), y='pnl')
        plt.savefig('hedge_pnl_{}_{}.pdf'.format(self.__date_beg, self.__date_end), bbox_inches='tight')
        
        return self
    
if __name__ == "__main__":
    
    kwargs = {"hostname": "kdb1", "portnum": 10101, "username": "ygao", "password": "Password23", \
              "f2f_database": "/data/db_sun_f2f", "f2f_fifo": "f2f_fifo", \
              "fut_database": "/data/db_tdc_us_futures_cme", "fut_book": "book"}
    
    date_beg = '2016.01.01'
    date_end = '2016.11.16'
    
    initiator = 'CH_HG_PB'
    type_name = 'PBM'
    
    ha = HedgeAnalysis(date_beg, date_end, initiator, type_name, **kwargs).get_liq_trd().analyze().save_df().plot_price().plot_pnl()
