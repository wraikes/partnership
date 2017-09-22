import os, pandas as pd, numpy as np, re
from functools import reduce


#def main():
sage = '/home/wraikes/Dropbox/partnership/dmt/data/sage_not_final'
#sage = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp\Sage (Mobile Phone) Data 8.31.17'
os.chdir(sage)
files_to_exclude = ['digital-marshmallow-status_8.31.17.csv',
                    'digital-marshmallow-appVersion_8.31.17.csv']
end_string = '_8.8.17'

    
def merge_files(directory, exclude, end_string):
    '''
    Go through and process all files in directory.
    If extra processing is needed, use a special function 'new_df_func'.
    Return a single dataframe, of all data merged.
    '''
    
    new_dfs = []
    
    for file in directory:
        name = create_df_name(file, end_string)
        
        if file not in exclude:
            df = pd.read_csv(file)
            df = remove_dupes(df)
            if len(df.externalId) == len(df.externalId.unique()):
                df = new_cols(df, name)          
                new_dfs.append(df)
            
            else:
                df = new_df_func(df, name)
                new_dfs.append(df)
    
    final_df = reduce(lambda left, right: pd.merge(left, right, how = 'outer',
                                                   on='externalId'), 
                      new_dfs)
    
    return final_df


def new_df_func(df, df_name):
    '''
    Each dataframe is cleaned with special instructions based on the filters.
    A cleaned dataframe is returned.
    '''
    
    if df_name == 'bart-v4':
        attributes = [
            ['baseline', 'BART0.25'],
            ['baseline', 'BART250.00'],
            ['21-day-assessment', 'BART0.25'],
            ['21-day-assessment', 'BART250.00']
        ]
                
        df = df_merge(df, df_name, attributes, 
                      var_1='metadata.json.taskIdentifier', 
                      var_2='data.json.variable_label')
        
    elif df_name == 'delay_discounting_raw-v6':
        bl = 'baseline'
        _21 = '21-day-assessment'
        
        attributes = [
            [bl, 'dd_time_6_months'],
            [bl, 'dd_money_6_month'],
            [bl, 'dd_money_1_month'],
            [bl, 'dd_time_1_year'],
            [_21, 'dd_time_6_month'],
            [_21, 'dd_money_6_month'],
            [_21, 'dd_money_1_month'],
            [_21, 'dd_time_1_year']            
        ]
        
        df = df_merge(df, df_name, attributes, 
                      var_1='metadata.json.taskIdentifier',
                      var_2='data.json.variableLabel')

#    elif df_name == '':
#        attributes = []
#        
#        df = df_merge()
#    
#    elif df_name == '':
#        attributes = []
#        
#        df = df_merge()
#    
#    
#    elif df_name == '':
#        attributes = []
#        
#        df = df_merge()
#   
#
    return df

    
def df_merge(df, df_name, attributes, var_1, var_2=None):
    '''
    Merge the dataframes into one, with new columns.
    '''
    
    dfs = []
    
    for att in attributes:
        new_df = create_new_df(df, att, var_1, var_2)
        new_df = col_relabel(new_df, df_name)
        dfs.append(new_df)
    
    df_merge = reduce(lambda left, right: pd.merge(left, 
                                                   right, 
                                                   how = 'outer',
                                                   on='externalId'),
                      dfs)
    
    return df_merge


def new_df(df, att, var_1, var_2=None):
    '''
    Create a new dataframe based on the filters below
    '''
    
    cols = df.columns
    
    new_df = pd.DataFrame(columns=cols)
        
    for ix, row in df.iterrows():
        if var_2:
            if row[var_1] == att[0] and row[var_2] == att[1]:
                new_df = new_df.append(row, ignore_index=True)
        else:
            if row[var_1] == att[0]:
                new_df = new_df.append(row, ignore_index=True)
    
    return new_df


def col_relabel(df, append):
    '''
    Remove: 'metadata.json.' and 'data.json.'.
    Replace spaces with underscores.
    Append new name to existing column name, except if externalId.
    '''
    new_cols = []
    col_re = re.compile('metadata.json.|data.json.')
    
    for col in df.columns:
        new_col = re.sub(col_re, '', col)
        new_col = new_col.replace(' ', '_')
        new_col = new_col.replace('-', '_')
        if new_col != 'externalId':
            new_col = append + new_col
    
    return df

def create_df_name(string, end_string):
    string = string.replace('digital-marshmallow-', '')
    string = string.replace(end_string, '')
    
    return string        
            

#Functions for data processing
def remove_dupes(df):
    
    test_users = [
        'ThpMV2Achc', 
        'SEkQVTCe6j', 
        'Wh8NSX3DHL', 
        'SaXFr2kPZa', 
        'VWUcSp4TeH', 
        'yXEfAmW682', 
        'gwEpQR8j9B',
        'WbbNWM4RAF', 
        'D5bzYrfd8E', 
        'LJcmEFWp74', 
        'ULoF3MM1nN'
    ]

    diff_study = [
        'rL8eA3',
        'rLg5xs',
        'rLrD9h',
        'rLP7H2',
        'rL6s6h',
        'aOyzBg',
        'aORA43',
        'aOh48U',
        'aOLu4K',
        'aOQtxv',
        'aO5TvQ',
        'mPC9S8',
        'mPgquX',
        'mP5xkB',
        'mPSQvh',
        'mPYk2p',
        'mP3rbd'
    ]

    
    return df[~df.externalId.isin(test_users + diff_study)]

#def dupe_check(df):
#    return len(df.externalId) == len(df.externalId.unique()) and len(df.externalId) > 0


#test = merge_files(os.listdir(), files_to_exclude, end_string)





