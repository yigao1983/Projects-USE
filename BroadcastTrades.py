import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import smtplib

class BroadcastTrades(object):
    
    def __init__(self, **kwargs):
        
        self.__hostname      = kwargs["hostname"]
        self.__portnum       = kwargs["portnum"]
        self.__username      = kwargs["username"]
        self.__password      = kwargs["password"]
        self.__nbbo_database = kwargs["nbbo_database"]
        self.__nbbo_trades   = kwargs["nbbo_trades"]
        self.__nbbo_quotes   = kwargs["nbbo_quotes"]
        self.__itch_database = kwargs["itch_database"]
        self.__itch_trade    = kwargs["itch_trade"]
        
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
            
            self.__q.sync('\l {}'.format(self.__nbbo_database))
            self.__q.sync('last_date:last date;')
            self.__q.sync('trade_tab:.st.unenum select volume:sum size,price:size wavg price by sym from {} where date=last_date,'
                          'price>1,price<1000,'
                          'sun_time within (08:30:00,15:00:00),not sym like "*ZZT",not sym like "*.TEST",'
                          'not sym in `CBO`CBX;'.format(self.__nbbo_trades))
            self.__q.sync('sym_lst:distinct exec sym from trade_tab;')
            self.__q.sync('quote_tab:.st.unenum select spread:med ask_price-bid_price by sym from {} where date=last_date,'
                          'sun_time within (08:30:00,15:00:00),(sym) in (sym_lst);'.format(self.__nbbo_quotes))
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
    
    def get_spy_trade(self):
        
        try:
            self.__q.sync('\l {}'.format(self.__itch_database))
            self.__q.sync('last_date:last date')
            self.__q.sync('spy_tab:.st.unenum select sym,sun_time,price from {} where date=last_date,'
                          'sym=`SPY,sun_time within (08:00:00,15:00:00),flags="D";'.format(self.__itch_trade))
            self.__q.sync('spy_tab:.st.unenum select from spy_tab where spy_tab[i;`price]<>spy_tab[i-1;`price]')
            
            self.__last_date = self.__q.sync('last_date')
            self.__spy_df = self.__q.sync('spy_tab', pandas=True)
            
        except Exception as e:
            print(e)
        
        return self
    
    def spy_accuracy(self, price_series, dprice):
        
        self.__dprice = dprice
        
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
    
    def broadcast(self):
        
        sender = 'ygao@suntradingllc.com'
        receivers = ['ygao@suntradingllc.com']
        
        message = """
        Dear all,
        
        Statistics of last trading day ({}) is
        
        Volume weighted averge spread: ${:<10.4f}
        SPY 5-tick momentum accuracy:   {:<10.4f}
        
        Best wishes!
        
        Yi
        """.format(self.__last_date, self.vol_avg_spread(), self.spy_accuracy(self.__spy_df.price, 0.05))
         
        try:
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, message)
            print("Successfully sent email")
        except smtplib.SMTPException:
            print("Error: cannot send email")

if __name__ == "__main__":
    
    kwargs = {"hostname": "kdb1", "portnum": 10101, "username": "ygao", "password": "Password23", \
              "nbbo_database": "/data/db_tdc_us_equities_nbbo", "nbbo_trades": "trades", "nbbo_quotes": "quotes", \
              "itch_database": "/data/db_tdc_us_equities_itch", "itch_trade": "trade"}
    
    bt = BroadcastTrades(**kwargs).get_trade_quote().get_symbol_list("symbol_list.csv").get_spy_trade().broadcast()
