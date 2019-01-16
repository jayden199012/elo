import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer


# directory and name for all dataframes
raw_data_dir = '../1RawData/'
data_list = {"hist_trans": raw_data_dir + "historical_transactions.csv",
             "merchants": raw_data_dir + "merchants.csv",
             "new_merchants": raw_data_dir + "new_merchant_transactions.csv",
             "train": raw_data_dir + "train.csv",
             "test": raw_data_dir + "test.csv",
             "sample_sub": raw_data_dir + "sample_submission.csv"
             }

# dictionry that contains all data
data = {}

# the data was too large to open, so I created the dataframe dictionary to
# examine the data
data_head = {}

# load dataframe into dictionary
for index  in data_list:
    data[index] = pd.read_csv(data_list[index])
    data_head[index] = data[index].head(100)

# description dictionary including null value counts and data type
description = {}
pd.set_option("display.max_columns", 100)
for df in data:
    description[df] = data[df].describe(include='all').append(
                        [data[df].isnull().sum().rename('null_vals'),
                         data[df].dtypes.rename('data_types')])
len(data['test'].card_id.unique())
len(data['test'].card_id)
# columns with missing values for each dataframe
missing_columns = {}
for df, des in description.items():
    missing_columns[df] = des.columns[des.loc['null_vals'] > 0]

#count_impute = SimpleImputer(strategy='most_frequent')
#for df in [data['hist_trans'], data['new_merchants']]:
#    count_impute.fit_transform(df.loc[:, ['category_3', 'merchant_id',
#                                          'category_2']])


# new columns related to dt
new_col_name = ['year', 'month', 'weekofyear', 'dayofweek', 'hour']
# feature engineer for hist trans and new merchants df
for df in [data['hist_trans'], data['new_merchants']]:
    df['category_2'].fillna(1.0,inplace=True)
    df['category_3'].fillna('A',inplace=True)
    df['merchant_id'].fillna('M_ID_00a6ca8a8a',inplace=True)
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
#    df['category_1'] = df['category_1'].map({'Y':1, 'N': 0})
#    df['authorized_flag'] = df['authorized_flag'].map({'Y':1, 'N':0})
#    df['category_3'] = pd.factorize(df['category_3'], sort=True)
    for col_name in new_col_name:
        df[col_name] = getattr(df['purchase_date'].dt, col_name)
    df['weekend'] = (df.dayofweek > 5).astype(int)

# agg
    
aggs = {}
for col in ['month','hour','weekofyear','dayofweek','year','subsector_id','merchant_id','merchant_category_id']:
    aggs[col] = ['nunique']

aggs['purchase_amount'] = ['sum','max','min','mean','var']
aggs['installments'] = ['sum','max','min','mean','var']
aggs['purchase_date'] = ['max','min']
aggs['month_lag'] = ['max','min','mean','var']
aggs['authorized_flag'] = ['sum', 'mean']
aggs['weekend'] = ['sum', 'mean']
aggs['category_1'] = ['sum', 'mean']
aggs['card_id'] = ['size']
def get_new_columns(name,aggs):
    return [name + '_' + k + '_' + agg for k in aggs.keys() for agg in aggs[k]]
new_columns = get_new_columns('hist',aggs)
df_hist_trans_group = data['hist_trans'].groupby('card_id').agg(aggs)
data['hist_trans']['authorized_flag']
data_head['hist_trans'][' = df['authorized_flag'].map({'Y':1, 'N':0})
