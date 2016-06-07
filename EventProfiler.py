__author__ = 'rbaraldi'

import pandas as pd
import numpy as np
import math
import copy
import QSTK.qstkutil.qsdateutil as du
import datetime as dt
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkstudy.EventProfiler as ep
import thread
import time


dataObj = da.DataAccess('Yahoo')


def find_events(ls_symbols, d_data, f_priceDrop):
    ''' Finding the event dataframe '''
    df_close = d_data['actual_close']

    print "\nFinding Events"

    # Creating an empty dataframe
    df_events = copy.deepcopy(df_close)
    df_events = df_events * np.NAN

    # Time stamps for the event range
    ldt_timestamps = df_close.index

    for s_sym in ls_symbols:
        for i in range(1, len(ldt_timestamps)):
            # Calculating the returns for this timestamp
            f_symprice_today = df_close[s_sym].ix[ldt_timestamps[i]]
            f_symprice_yest = df_close[s_sym].ix[ldt_timestamps[i - 1]]

            # Event is found if on 2 consecutive closes the price went from
            # greater than or equal to 5.00 to less than 5.00
            if f_symprice_yest >= f_priceDrop and f_symprice_today < f_priceDrop:
                df_events[s_sym].ix[ldt_timestamps[i]] = 1

    return df_events


def create_study(ls_symbols, ldt_timestamps, s_study_name, lf_priceDrop):
    global dataObj

    print "\nGrabbing data to perform {0}".format(s_study_name)
    ls_keys = ['close', 'actual_close']
    ldf_data = dataObj.get_data(ldt_timestamps, ls_symbols, ls_keys)

    print "\nGot data for study {0}".format(s_study_name)
    d_data = dict(zip(ls_keys, ldf_data))

    for s_key in ls_keys:
        d_data[s_key] = d_data[s_key].fillna(method='ffill')
        d_data[s_key] = d_data[s_key].fillna(method='bfill')
        d_data[s_key] = d_data[s_key].fillna(1.0)

    for f_priceDrop in lf_priceDrop:
        df_events = find_events(ls_symbols, d_data, f_priceDrop)

        new_study_name = s_study_name + str(f_priceDrop).replace(".","_") + ".pdf"
        print "Creating Study {0}".format(new_study_name)
        ep.eventprofiler(df_events, d_data, i_lookback=20, i_lookforward=20,
                         s_filename=new_study_name, b_market_neutral=True, b_errorbars=True,
                         s_market_sym='SPY')


def main():
    dt_start = dt.datetime(2008, 1, 1)
    dt_end = dt.datetime(2009, 12, 31)
    ldt_timestamps = du.getNYSEdays(dt_start, dt_end, dt.timedelta(hours=16))

    global dataObj

    ls_symbols_2012 = dataObj.get_symbols_from_list('sp5002012')
    ls_symbols_2012.append('SPY')

    ls_symbols_2008 = dataObj.get_symbols_from_list('sp5002008')
    ls_symbols_2008.append('SPY')

    lf_priceDrop2008 = [7.0, 8.0, 10.0]
    lf_priceDrop2012 = [6.0, 7.0, 9.0, 10.0]

    try:
        #thread.start_new_thread(create_study, (ls_symbols_2008, ldt_timestamps, '2008StudyPriceDrop',lf_priceDrop2008))
        #thread.start_new_thread(create_study, (ls_symbols_2012, ldt_timestamps, '2012StudyPriceDrop',lf_priceDrop2012))
        create_study(ls_symbols_2008, ldt_timestamps, '2008StudyPriceDrop',lf_priceDrop2008)
        create_study(ls_symbols_2012, ldt_timestamps, '2012StudyPriceDrop',lf_priceDrop2012)
    except:
        print "Error: unable to start thread"

if __name__ == '__main__':
    main()