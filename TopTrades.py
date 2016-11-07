import os
import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import matplotlib.pyplot as plt

class TopTrades(object):
    
    def __init__(self, date_beg, date_end, num_top, **kwargs):
        
        self.__date_beg = str(date_beg)
        self.__date_end = str(date_end)
        self.__num_top  = num_top
        
        self.__hostname = kwargs["hostname"]
        self.__portnum  = kwargs["portnum"]
        self.__username = kwargs["username"]
        self.__password = kwargs["password"]
        self.__database = kwargs["database"]
        self.__trades   = kwargs["trades"]
        self.__quotes   = kwargs["quotes"]
        
        self.__q = qconn.QConnection(host=self.__hostname, port=self.__portnum, \
                                     username=self.__username, password=self.__password)
        
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
    
    def get_trade(self):
        
        try:
            
            self.__q.sync('\l {}'.format(self.__database))
            self.__q.sync('date_beg:{};'.format(self.__date_beg))
            self.__q.sync('date_end:{};'.format(self.__date_end))
            self.__q.sync('num_top:{}'.format(self.__num_top))
            self.__q.sync('trade_tab:.st.unenum select volume:sum size,notional:sum size*price,price:(size*price) wavg price '
                          'by 1 xbar date,sym from {} where date within (date_beg,date_end),sun_time>08:30:00,sun_time<15:00:00,'
                          'price>1,not sym like "*ZZT",not sym like "*.TEST",not sym in `CBO`CBX;'.format(self.__trades))
            self.__q.sync('sym_lst:distinct exec sym from num_top#`tot_notional xdesc .st.unenum select tot_notional:sum notional '
                          'by sym from trade_tab;')
            self.__q.sync('trade_tab:.st.unenum select from trade_tab where (sym) in (sym_lst);')
            
            self.__tr_df = self.__q.sync('trade_tab', pandas=True)
            
        except Exception as e:
            print(e)
    
    def get_quote(self):
        
        try:
            
            self.__q.sync('\l {}'.format(self.__database))
            self.__q.sync('date_beg:{};'.format(self.__date_beg))
            self.__q.sync('date_end:{};'.format(self.__date_end))
            
            self.__q.sync('quote_tab:.st.unenum select spread:med ask_price-bid_price by date,sym from {} '
                          'where date within (date_beg,date_end),sun_time>08:30:00,sun_time<15:00:00,'
                          '(sym) in (sym_lst);'.format(self.__quotes))
            
            self.__qt_df = self.__q.sync('quote_tab', pandas=True)
            
        except Exception as e:
            print(e)
    
    def get_trade_quote(self, tq_csv=None):
        
        if tq_csv and os.path.isfile(tq_csv):
            
            self.__tq_df = pd.read_csv(tq_csv)
            self.__tq_df.date = pd.to_datetime(self.__tq_df.date, format="%Y-%m-%d")
            self.__tq_df.set_index(['date', 'sym'], drop=True, inplace=True)
            print('Successfully read trade-quote dataframe.')
            
        else:
            self.get_trade()
            self.get_quote()
            
            self.__tq_df = pd.merge(self.__tr_df, self.__qt_df, left_index=True, right_index=True, how='inner')
            self.__tq_df['spread_to_price'] = self.__tq_df.spread / self.__tq_df.price
            
            self.__tq_df.to_csv('tq_df_{}_{}.csv'.format(self.__date_beg, self.__date_end)) 
    
    def get_spread(self):
        
        wa = lambda x: np.average(x, weights=self.__tq_df.loc[x.index, 'notional'])
        ff = {'price': wa, 'spread_to_price': wa}
        
        self.__sp_df = self.__tq_df.groupby(level=0).agg(ff)
        
        self.__sp_df.to_csv('sp_df_{}_{}.csv'.format(self.__date_beg, self.__date_end))
        
    def plot_spread(self):
        
        self.__sp_df.plot(y='spread_to_price', fontsize=12)
        plt.savefig('spread_to_price_{}_{}.pdf'.format(self.__date_beg, self.__date_end), bbox_inches='tight')
        
        self.__sp_df.plot(y='price', fontsize=12)
        plt.savefig('price_{}_{}.pdf'.format(self.__date_beg, self.__date_end), bbox_inches='tight')
    
if __name__ == "__main__":
    
    date_beg = '2015.01.01'
    date_end = '2015.12.31'
    num_top = 10000
    
    kwargs = {"hostname": "kdb1", "portnum": 10101, "username": "ygao", "password": "Password23",
              "database": "/data/db_tdc_us_equities_nbbo", "trades": "trades", "quotes": "quotes"}
    
    top = TopTrades(date_beg, date_end, num_top, **kwargs)
    top.get_trade_quote('tq_df_{}_{}.csv'.format(date_beg, date_end))
    top.get_spread()
    
    top.plot_spread()
