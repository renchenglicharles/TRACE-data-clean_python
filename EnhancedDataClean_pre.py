##########################
#
# PRE 2012 change
#
##########################
import pandas as pd
import numpy as np

data_raw = pd.read_csv('TRACE2012pre.csv',header=0)
print('data_raw.info=')
data_raw.info()
# Takes same-day corrections and splits them into two data sets;
# 1 for all the correct trades, and 1 for the corrections;

# Deletes observations without a cusip_id;

data_raw.dropna(subset=['cusip_id'],inplace=True)
print('Delete obervations without a cusip_id')
data_raw.info()

# Takes out all cancellations into the temp_delete dataset;

temp_delete = data_raw.loc[data_raw['trc_st'].isin(['C','W'])]
temp_delete.info()

#All corrections are put into both datasets;

temp_raw = data_raw.loc[~data_raw['trc_st'].isin(['C'])]
temp_raw.info()

# Deletes the error trades as identified by the message
# sequence numbers. Same day corrections and cancelations;


cols = list(temp_delete.columns)
a, b = cols.index('msg_seq_nb'), cols.index('orig_msg_seq_nb')
cols[b], cols[a] = cols[a], cols[b]
temp_delete = temp_delete[cols]
temp_delete.rename(columns={'msg_seq_nb':'orig_msg_seq_nb', 'orig_msg_seq_nb':'msg_seq_nb'}, inplace=True)
temp_raw2 = temp_raw.append(temp_delete)
temp_raw2 = temp_raw2.drop_duplicates(subset=['msg_seq_nb','trd_rpt_dt','trd_rpt_tm','cusip_id'],keep=False)


print('Delete the same day corrections and cancelations')
temp_raw2.info()


# Take out reversals into a dataset;

reversal = temp_raw2.loc[temp_raw2['asof_cd'].isin(['R'])]
print('Reversals')
reversal.info()
temp_raw3 = temp_raw2.loc[~temp_raw2['asof_cd'].isin(['R'])]
print('temp_raw3')
temp_raw3.info()


# Sorting the data so that it can be merged;

reversal = reversal.drop_duplicates(subset=['trd_exctn_dt','cusip_id','trd_exctn_tm','rptd_pr','entrd_vol_qt','rpt_side_cd','cntra_mp_id','trd_rpt_dt','trd_rpt_tm','msg_seq_nb'],keep='first')
print('Reversals after droping duplicates')
reversal.info()
# Merges reversals back on and selects matching observations;

reversal2 = pd.merge(temp_raw3,reversal,how='inner',on=['trd_exctn_dt','cusip_id','trd_exctn_tm','rptd_pr','entrd_vol_qt','rpt_side_cd','cntra_mp_id'],suffixes=['','_x'])
reversal2.info()
# Reversal have to be on a later date
# (or else it would not be a reversal);
# i.e. we do not delete potential as_of trades
# from a later date that may match;

reversal2 = reversal2[reversal2['trd_exctn_dt']<reversal2['trd_rpt_dt_x']]

# Selects only 1 matching reversal (and keeps the rest);

reversal2 = reversal2.drop_duplicates(subset=['bond_sym_id','entrd_vol_qt','rptd_pr','trd_exctn_dt','trd_exctn_tm'],keep='first')
reversal_col = pd.DataFrame(reversal2,columns=data_raw.columns.values)
print('reversal_col')
reversal_col.info()

# Deletes the matching reversals;

temp_raw4 = temp_raw3.append(reversal_col)
temp_raw4 = temp_raw4.drop_duplicates(keep=False)
print('temp_raw4')
temp_raw4.info()

# Ends the filter for PRE-change data;


temp_raw4_col = temp_raw4.drop(['asof_cd','trd_rpt_dt','trd_rpt_tm','msg_seq_nb','trc_st','orig_msg_seq_nb'],axis=1)
temp_raw4_col.to_csv('CleanTRACE2012pre.csv',index=False)
print('Data clean')