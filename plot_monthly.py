import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm

def main():
    
    monthly_df = pd.read_csv('monthly_data.csv', delim_whitespace=True)
    monthly_df.Month = pd.to_datetime(monthly_df.Month, format='%Y%m')
    monthly_df.set_index('Month', drop=True, inplace=True)
    print(monthly_df)
    
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    
    ax1.yaxis.tick_left()
    ax2.yaxis.tick_right()
    
    monthly_df.plot(y='NTR', color='k', ax=ax1, legend=True)
    ax1.legend(loc=2)
    
    monthly_df.plot(y='spread', color='r', ax=ax2, legend=True)
    ax1.legend(loc=3)
    
    plt.savefig('monthly.pdf', bbox_inches='tight')
    
    X = np.copy(monthly_df.spread.values)
    y = np.copy(monthly_df.NTR.values)
    print(np.corrcoef(y, X))
    result = sm.OLS(y, sm.add_constant(X)).fit()
    print(result.summary())
    
    plt.figure()
    plt.scatter(X, y)
    plt.show()
if __name__ == "__main__":
    
    main()
