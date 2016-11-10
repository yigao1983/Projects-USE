import os
import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import matplotlib.pyplot as plt

class TradeTicks(object):
    
    def __init__(self, date_beg, date_end, **kwargs):
        
        self.__date_beg = str(date_beg)
        self.__date_end = str(date_end)
        
        self.__hostname = kwargs["hostname"]
        self.__portnum  = kwargs["portnum"]
        self.__username = kwargs["username"]
        self.__password = kwargs["password"]
        self.__database = kwargs["database"]
        self.__trade    = kwargs["trade"]
        
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
    def tr_df(self):
        
        return self.__tr_df
    
    @property
    def cr_df(self):
        
        return self.__cr_df
    
    def get_trade(self, sym='SPY', dt=1, nlag=1):
        
        try:
            
            self.__sym  = sym
            self.__dt   = dt
            self.__nlag = nlag
            
            self.__q.sync('\l {}'.format(self.__database))
            self.__q.sync('date_beg:{};'.format(self.__date_beg))
            self.__q.sync('date_end:{};'.format(self.__date_end))
            self.__q.sync('dt:{}'.format(self.__dt))
            self.__q.sync('nlag:{}'.format(self.__nlag))
            self.__q.sync('trade_tab:([]date:();sym:();sun_time:();price:();ret:());')
            self.__q.sync('date_now:date_beg;')
            self.__q.sync('while[date_now<=date_end;'
                          'tab:.st.unenum select date,sym,sun_time,price from {} where date=date_now,sym=`{},sun_time within(08:30:00,15:00:00),flags="D";'
                          'tab:.st.unenum select from tab where tab[i;`price]<>tab[i-1;`price];'
                          'update ret:-1+price%(dt xprev price) from `tab;trade_tab:trade_tab,tab;'
                          'date_now:date_now+1;];`end;'.format(self.__trade, self.__sym))
            self.__q.sync('cor_tab:select sym:last sym,volatility:dev ret,autocor:cor[ret;nlag xprev ret] by date from trade_tab;')
            
            self.__tr_df = self.__q.sync('trade_tab', pandas=True)
            self.__cr_df = self.__q.sync('cor_tab', pandas=True)
            
            self.__tr_df.to_csv('tr_df_{}_{}_{}.csv'.format(self.__sym, self.__date_beg, self.__date_end))
            self.__cr_df.to_csv('cr_df_{}_{}_{}.csv'.format(self.__sym, self.__date_beg, self.__date_end))
        
        except Exception as e:
            print(e)
        
        return self
    
    def plot_cor(self):
        
        self.__cr_df.plot(y='volatility')
        plt.savefig('volatility_{}_{}_{}.pdf'.format(self.__sym, self.__date_beg, self.__date_end), bbox_inches='tight')
        
        self.__cr_df.plot(y='autocor')
        plt.savefig('autocor_{}_{}_{}.pdf'.format(self.__sym, self.__date_beg, self.__date_end), bbox_inches='tight')
        
        return self

if __name__ == "__main__":
    
    date_beg = '2015.01.01'
    date_end = '2016.10.31'
    
    kwargs = {"hostname": "kdb1", "portnum": 10101, "username": "ygao", "password": "Password23",
              "database": "/data/db_tdc_us_equities_itch", "trade": "trade"}
    
    tick = TradeTicks(date_beg, date_end, **kwargs).get_trade(dt=1, nlag=50).plot_cor()
