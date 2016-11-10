import os
import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import matplotlib.pyplot as plt

class TopTrades(object):
    
    def __init__(self, date_beg, date_end, **kwargs):
        
        self.__date_beg = str(date_beg)
        self.__date_end = str(date_end)
        
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
    
    @property
    def sym_df(self):
        
        return self.__sym_df
    
    def get_trade(self):
        
        try:
            
            self.__q.sync('\l {}'.format(self.__database))
            self.__q.sync('date_beg:{};'.format(self.__date_beg))
            self.__q.sync('date_end:{};'.format(self.__date_end))
            self.__q.sync('trade_tab:.st.unenum select volume:sum size,notional:sum size*price,price:(size*price) wavg price '
                          'by 1 xbar date,sym from {} where date within (date_beg,date_end),sun_time>08:30:00,sun_time<15:00:00,'
                          'price>1,not sym like "*ZZT",not sym like "*.TEST",not sym in `CBO`CBX;'.format(self.__trades))
            self.__q.sync('sym_lst:distinct exec sym from `tot_notional xdesc .st.unenum select tot_notional:sum notional '
                          'by sym from trade_tab;')
            self.__q.sync('trade_tab:.st.unenum select from trade_tab where (sym) in (sym_lst);')
            
            self.__tr_df = self.__q.sync('trade_tab', pandas=True)
            
        except Exception as e:
            print(e)
        
        return self
    
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
        
        return self
    
    def get_trade_quote(self, tq_csv):
        
        if tq_csv and os.path.isfile(tq_csv):
            
            print('File exists.')
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
        
        return self
    
    def get_spread(self):
        
        wn = lambda x: np.average(x, weights=self.__tq_df.loc[x.index, 'notional'])
        fn = {'price': wn, 'spread_to_price': wn}
        
        wv = lambda x: np.average(x, weights=self.__tq_df.loc[x.index, 'volume'])
        fv = {'price': wv, 'spread_to_price': wv}
        
        self.__sp_not_df = self.__tq_df.groupby(level=0).agg(fn)
        self.__sp_vol_df = self.__tq_df.groupby(level=0).agg(fv)
        
        self.__sp_not_df.to_csv('sp_not_df_{}_{}_{}.csv'.format(self.__date_beg, self.__date_end, self.__num_top))
        self.__sp_vol_df.to_csv('sp_vol_df_{}_{}_{}.csv'.format(self.__date_beg, self.__date_end, self.__num_top))
        
        return self
    
    def plot_spread(self):
        
        self.__sp_not_df.plot(y='spread_to_price', fontsize=12)
        plt.savefig('spread_to_price_not_{}_{}_{}.pdf'.format(self.__date_beg, self.__date_end, self.__num_top), bbox_inches='tight')
        self.__sp_not_df.plot(y='price', fontsize=12)
        plt.savefig('price_not_{}_{}_{}.pdf'.format(self.__date_beg, self.__date_end, self.__num_top), bbox_inches='tight')
        
        self.__sp_vol_df.plot(y='spread_to_price', fontsize=12)
        plt.savefig('spread_to_price_vol_{}_{}_{}.pdf'.format(self.__date_beg, self.__date_end, self.__num_top), bbox_inches='tight')
        self.__sp_vol_df.plot(y='price', fontsize=12)
        plt.savefig('price_vol_{}_{}_{}.pdf'.format(self.__date_beg, self.__date_end, self.__num_top), bbox_inches='tight')
        
        return self
    
    def get_symbol_list(self, symbol_csv, num_top):
        
        self.__num_top = num_top
        
        try:
            self.__sym_df = pd.read_csv(symbol_csv)
            ff = lambda x: self.__tq_df[self.__tq_df.index.get_level_values('sym')==x]['notional'].sum()
            self.__sym_df['notional'] = self.__sym_df['symbol'].apply(ff)
            self.__sym_df.sort_values(by='notional', ascending=False, inplace=True)
            self.__sym_df.reset_index(drop=True, inplace=True)
            self.__sym_df = self.__sym_df[self.__sym_df.index<self.__num_top]
            self.__sym_df.to_csv('red_symbol_list_{}.csv'.format(self.__num_top))
        except Exception as e:
            print(e)
        
        return self
    
    def clean_trade_quote(self):
        
        self.__tq_df = self.__tq_df[self.__tq_df.index.get_level_values('sym').isin(self.__sym_df['symbol'].values)]
        self.__tq_df = self.__tq_df[(self.__tq_df.spread_to_price<self.__tq_df.spread_to_price.quantile(0.99)) & \
                                    (self.__tq_df.price>1.0) & (self.__tq_df.price<1000.0)]
        self.__tq_df.to_csv('red_tq_df_{}_{}_{}.csv'.format(self.__date_beg, self.__date_end, self.__num_top))
        
        return self
    
if __name__ == "__main__":
    
    date_beg = '2014.01.01'
    date_end = '2016.10.31'
    num_top = 2000
    
    kwargs = {"hostname": "kdb1", "portnum": 10101, "username": "ygao", "password": "Password23",
              "database": "/data/db_tdc_us_equities_nbbo", "trades": "trades", "quotes": "quotes"}
    
    top = TopTrades(date_beg, date_end, **kwargs).get_trade_quote('tq_df_{}_{}.csv'.format(date_beg, date_end))
    top.get_symbol_list('symbol_list.csv', num_top).clean_trade_quote().get_spread().plot_spread()
