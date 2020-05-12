###################################################################
# Based on
#
# 20141201
# (C) Jens Dick-Nielsen
# Copenhagen Business School
# jdn.fi@cbs.dk
#
#
# Description:
# Takes an Enhanced TRACE data sample from WRDS and cleans out
# the reporting errors. See details in the note "How to clean
# Enhanced TRACE data". For more details on the algorithm see
# the article "Liquidity biases in TRACE".
#
# This's a python version of the clean algorithm by
#
#Ren Chengli
#Chinese University of Hong Kong
#clren@se.cuhk.edu.hk
#
# NB: The code contains some parts which are optional.
# These are by default not enabled but may be of use (see the
# last part of the code).
#
# Acknowledgement:
# Special thanks to Philipp Schuster, Karlsruhe Institute of
# Technology (KIT), for pointing out programming errors and
# suggested solutions in the earlier filter, and for drawing
# my attention to the 2012 change in TRACE. All remaining
# errors are my own.
#
###################################################################
# The name of the original Enhanced TRACE dataset
# supplied to the program;
#LET IN = traceIN;
# The name given to the cleaned Enhanced TRACE dataset;
#Let OUT = traceCLEAN;
# Path to your library containing the dataset;
# The cleaned dataset will be stored in this location;
import pandas as pd


###################################################################
#
# Start of the program
#
###################################################################

data_raw = pd.read_csv('TRACE2015.csv',header=0)
print('data_raw.info=')
data_raw.info()

##########################
#
# POST 2012 change
#
##########################
# Cleans data reported after Feb 6th, 2012;
# The coding and reporting structure changed with the transition
# to the TRAQS reporting system;
# Specifically, the link between a reversal and the original
# transaction is now unique and transperant;



# Deletes observations without a cusip_id;

data_raw.dropna(subset=['cusip_id'],inplace=True)
print('Delete obervations without a cusip_id')
data_raw.info()

# Takes out all cancellations and corrections;
# These transactions should be deleted together with the
# original report;

temp_deleteI_NEW = data_raw.loc[data_raw['trc_st'].isin(['C','X'])]
print('Takes out all cancellations and corrections')
temp_deleteI_NEW.info()

# Reversals. These have to be deteled as well together with
# the original report;

temp_deleteII_NEW = data_raw.loc[data_raw['trc_st'].isin(['Y'])]
print('Reversals')
temp_deleteII_NEW.info()

# The rest of the data;

temp_raw = data_raw.drop(data_raw[(data_raw['trc_st'] == 'C')|(data_raw['trc_st'] == 'X')|(data_raw['trc_st'] == 'Y')].index)
print('The rest of the data')
temp_raw.info()

# Deletes the cancellations and corrections as identified by
# the reports in temp_deleteI_NEW;
# These transactions can be matched by message sequence number
# and date. We furthermore match on cusip, volume, price, date,
# time, buy-sell side, contra party;
# This is as suggested by the variable description;


df1 = temp_raw.append(temp_deleteI_NEW)
temp_raw2 = df1.drop_duplicates(subset=['cusip_id','entrd_vol_qt','rptd_pr','trd_exctn_dt','trd_exctn_tm','rpt_side_cd','cntra_mp_id','msg_seq_nb'],keep=False)
print('Deletes the cancellations and corrections')
temp_raw2.info()

# Deletes the reports that are matched by the reversals;

df2= temp_raw2.append(temp_deleteII_NEW)
temp_raw3 = df2.drop_duplicates(subset=['cusip_id','entrd_vol_qt','rptd_pr','trd_exctn_dt','trd_exctn_tm','rpt_side_cd','cntra_mp_id','msg_seq_nb'],keep=False)
print('Deletes the reports that are matched by the reversals')
temp_raw3.info()

# Ends the filtering of the post-change data
# clean used columns of data

temp_raw3_col = temp_raw3.drop(['asof_cd','trd_rpt_dt','trd_rpt_tm','msg_seq_nb','trc_st','orig_msg_seq_nb'],axis=1)

temp_raw3_col.to_csv('CleanTRACE2015.csv',index=False)
print('Data clean')
