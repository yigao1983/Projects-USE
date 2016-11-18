import sys
import numpy as np
import pandas as pd
import qpython.qconnection as qconn
import smtplib
import email.mime.multipart as multipart
import email.mime.text as text

class Broadcaster(object):
    
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
    
    @property
    def spy_df(self):
        
        return self.__spy_df
    
    def get_trade_quote(self, date_beg, date_end):
        
        try:
            
            self.__q.sync('\l {}'.format(self.__nbbo_database))
            self.__q.sync('date_beg:{}'.format(date_beg))
            self.__q.sync('date_end:{}'.format(date_end))
            self.__q.sync('trade_tab:.st.unenum select volume:sum size,price:size wavg price by sym from {} '
                          'where date within (date_beg,date_end),sun_time within (08:30:00.000000,15:00:00.000000),'
                          'price>1,price<1000,'
                          'not sym like "*ZZT",not sym like "*.TEST",not sym in `CBO`CBX;'.format(self.__nbbo_trades))
            self.__q.sync('sym_lst:distinct exec sym from trade_tab;')
            self.__q.sync('quote_tab:.st.unenum select spread:ask_price-bid_price by sym from {} '
                          'where date within (date_beg,date_end),sun_time within (08:30:00.000000,15:00:00.000000),'
                          '(sym) in (sym_lst);'.format(self.__nbbo_quotes))
            self.__q.sync('update spread:each [med] spread from `quote_tab;')
            self.__q.sync('tq_tab:ij[trade_tab;quote_tab];')
            
        except Exception as e:
            print(e)
        
        return self
    
    def vol_avg_spread(self, date_beg, date_end):
        
        self.get_trade_quote(date_beg, date_end)
        
        try:
            self.__tq_df = self.__q.sync('tq_tab', pandas=True)
            self.__tq_df = self.__tq_df[self.__tq_df.spread<self.__tq_df.spread.quantile(0.99)]
            self.__tq_df = pd.merge(self.__sym_df, self.__tq_df, left_on='symbol', right_index=True)
            
            return np.average(self.__tq_df.spread, weights=self.__tq_df.volume)
        
        except Exception as e:
            print(e)
    
    def get_symbol_list(self, symbol_csv):
        
        try:
            self.__sym_df = pd.read_csv(symbol_csv)
        except Exception as e:
            print(e)
        
        return self
    
    def get_spy_trade(self):
        
        try:
            self.__q.sync('\l {}'.format(self.__itch_database))
            self.__q.sync('last_date:last date')
            self.__q.sync('spy_tab:.st.unenum select sym,sun_time,price from {} where date=last_date,'
                          'sym=`SPY,sun_time within (08:30:00.000000,15:00:00.000000),flags="D";'.format(self.__itch_trade))
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
        
        try:
            # Last trading day spread
            self.__q.sync('\l {}'.format(self.__nbbo_database))
            date_beg = pd.to_datetime(self.__q.sync('last date')).strftime('%Y.%m.%d')
            date_end = pd.to_datetime(self.__q.sync('last date')).strftime('%Y.%m.%d')
            print(date_beg)
            print(date_end)
            spread_last = self.vol_avg_spread(date_beg, date_end)
            # Previous 5 trading day spread, excluding latest trading day
            date_beg_5 = pd.to_datetime(self.__q.sync('date[(count date)-5]')).strftime('%Y.%m.%d')
            date_end_5 = pd.to_datetime(self.__q.sync('date[(count date)-2]')).strftime('%Y.%m.%d')
            print(date_beg_5)
            print(date_end_5)
            spread_last_5 = self.vol_avg_spread(date_beg_5, date_end_5)
            # Previous 20 trading day spread, excluding latest trading day
            date_beg_20 = pd.to_datetime(self.__q.sync('date[(count date)-20]')).strftime('%Y.%m.%d')
            date_end_20 = pd.to_datetime(self.__q.sync('date[(count date)-6]')).strftime('%Y.%m.%d')
            print(date_beg_20)
            print(date_end_20)
            spread_last_20 = self.vol_avg_spread(date_beg_20, date_end_20)
        except Exception as e:
            print(e)
        
        sender = 'ygao@suntradingllc.com'
        receivers = ['ygao@suntradingllc.com', 'zluo@suntradingllc.com']
        
        message = multipart.MIMEMultipart('alternative')
        message['Subject'] = 'Market Statistics of Recent Trading Days'
        
        #Volume weighted average spread of last trading day: ${:<10.4f}
        main_text = """
        Volume weighted average spread of last trading day: ${:<10.4f}
        Volume weighted average spread of recent 5 trading days: ${:<10.4f}
        Volume weighted average spread of recent 20 trading days: ${:<10.4f}
        
        SPY 5-tick momentum index of last trading day: {:<10.4f}
        """.format(spread_last, spread_last_5, spread_last_20, self.spy_accuracy(self.__spy_df.price, 0.05))
        
        message.attach(text.MIMEText(main_text, 'plain'))
        
        try:
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, message.as_string())
            smtpObj.quit()
            print("Successfully sent email")
        except smtplib.SMTPException:
            print("Error: cannot send email")
    
if __name__ == "__main__":
    
    kwargs = {"hostname": "kdb1", "portnum": 10101, "username": "ygao", "password": "Password23", \
              "nbbo_database": "/data/db_tdc_us_equities_nbbo", "nbbo_trades": "trades", "nbbo_quotes": "quotes", \
              "itch_database": "/data/db_tdc_us_equities_itch", "itch_trade": "trade"}
    
    bc = Broadcaster(**kwargs).get_symbol_list('symbol_list.csv').get_spy_trade().broadcast()
