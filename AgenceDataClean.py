# This step deletes agency transactions but is not part
# of the error detection filter. This step can be deleted if
# you want to keep all agency transactions;
# Deletes agency customer transactions without commission;
# These transactions will have the same price as the
# interdealer transaction (if reported correctly);

import pandas as pd
import gc

data_raw = pd.read_csv('testagence.csv', header=0)


print('data_raw.info=')
data_raw.info()


# Identifies agency transactions;
# Deletes agency transactions which are dealer-customer
# transactions without commission;

temp_Buyside = data_raw.drop(data_raw[(data_raw['rpt_side_cd'].isin(['B'])) & (data_raw['cntra_mp_id'].isin(['C'])) & (data_raw['cmsn_trd'].isin(['N'])) & (data_raw['buy_cpcty_cd'].isin(['A']))].index)
temp_Buyside.info()

# Reversals. These have to be deteled as well together with
# the original report;

temp_Sellside = temp_Buyside.drop(temp_Buyside[(temp_Buyside['rpt_side_cd'].isin(['S'])) & (temp_Buyside['cntra_mp_id'].isin(['C'])) & (temp_Buyside['cmsn_trd'].isin(['N']))& (temp_Buyside['sell_cpcty_cd'].isin(['A']))].index)
temp_Sellside.info()

# The rest of the data;

print('The rest of the data')
temp_Sellside.info()

temp_col = temp_Sellside.drop(['cntra_mp_id'], axis=1)

temp_col.to_csv('Cleantestagence.csv', index=False)
print('Data clean')