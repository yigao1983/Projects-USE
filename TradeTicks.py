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
    
    def get_trade(self, sym='SPY'):
        
        self.__sym = sym
        
        trade_csv = 'tr_df_{}_{}_{}.csv'.format(self.__sym, self.__date_beg, self.__date_end)
        
        if os.path.isfile(trade_csv):
           
           self.__tr_df = pd.read_csv(trade_csv)
           
           print('Successful reading tr_df_{}_{}_{}.csv'.format(self.__sym, self.__date_beg, self.__date_end))
        
        else:
            
            try:
                
                self.__q.sync('\l {}'.format(self.__database))
                self.__q.sync('date_beg:{};'.format(self.__date_beg))
                self.__q.sync('date_end:{};'.format(self.__date_end))
                self.__q.sync('trade_tab:([]date:();sym:();sun_time:();price:());')
                self.__q.sync('date_now:date_beg;')
                self.__q.sync('while[date_now<=date_end;'
                              'tab:.st.unenum select date,sym,sun_time,price from {} where date=date_now,sym=`{},'
                              'sun_time within (08:30:00.000000,15:00:00.000000),flags="D";')
                self.__q.sync('tab:.st.unenum select from tab where tab[i;`price]<>tab[i-1;`price];'
                              'trade_tab:trade_tab,tab;'
                              'date_now:date_now+1;];`end;'.format(self.__trade, self.__sym))
                
                self.__tr_df = self.__q.sync('trade_tab', pandas=True)
            
            except Exception as e:
                print(e)
        
        self.__tr_df.date = pd.to_datetime(self.__tr_df.date, format='%Y-%m-%d')
        self.__tr_df.set_index(['date', 'sun_time'], drop=True, inplace=True)
        
        if not os.path.isfile(trade_csv):
            self.__tr_df.to_csv(trade_csv)
        
        return self
    
    def get_accuracy(self, price_series):
        
        xmax, idxmax = price_series.iloc[0], 0
        xmin, idxmin = xmax, idxmax
        # Find first large variation
        for idx, x in enumerate(price_series):
            if x>xmax:
                xmax, idxmax = x, idx
            if x<xmin:
                xmin, idxmin = x, idx
            if np.abs(xmax-xmin)>=self.__dprice:
                break
        # Indexes of variation points
        idxarray = []
        # First two
        (idx0, idx1) = (idxmax, idxmin) if idxmin>idxmax else (idxmin, idxmax)
        idxarray.extend([idx0, idx1])
        # All the rest
        for idx in range(idx1, price_series.size):
            if np.abs(price_series.iloc[idx]-price_series.iloc[idx1])>=self.__dprice:
                idx1 = idx
                idxarray.append(idx1)
        
        vararray = pd.Series(price_series.iloc[idxarray]).diff().dropna()
        predarray = vararray*vararray.shift(1)>0
        
        acc = np.count_nonzero(predarray) / (predarray.size-1.0) if predarray.size>1 else np.nan
        
        return acc
    
    def get_correlation(self, dprice=0.05):
        
        self.__dprice = dprice
        
        ff = {'sym': (lambda x: x[-1]), 'price': self.get_accuracy}
        
        self.__corr_df = self.__tr_df.groupby(level=0)[['sym', 'price']].agg(ff).rename(columns={'price': 'accuracy'})
        self.__corr_df.to_csv('corr_df_{}_{}_{}.csv'.format(self.__sym, self.__date_beg, self.__date_end))
        
        return self
    
    def plot_correlation(self):
        
        self.__corr_df.plot(y='accuracy')
        plt.savefig("correlation_{}_{}_{}.pdf".format(self.__sym, self.__date_beg, self.__date_end), bbox_inches="tight")
        
        return self

if __name__ == "__main__":
    
    date_beg = '2015.01.01'
    date_end = '2016.10.31'
    
    kwargs = {"hostname": "kdb2", "portnum": 10102, "username": "ygao", "password": "Password23",
              "database": "/data/db_tdc_us_equities_itch", "trade": "trade"}
    
    tick = TradeTicks(date_beg, date_end, **kwargs).get_trade().get_correlation(dprice=0.05).plot_correlation()
