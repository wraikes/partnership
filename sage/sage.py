import os, pandas as pd, numpy as np, re
from functools import reduce


def main():
    sage = '/home/wraikes/Dropbox/partnership/dmt/data/sage_not_final'
    #sage = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp\Sage (Mobile Phone) Data 8.31.17'
    os.chdir(sage)
    files_to_exclude = ['digital-marshmallow-status_8.31.17.csv',
                        'digital-marshmallow-appVersion_8.31.17.csv']
    end_string = '_8.31.17.csv'
    merge_files(os.listdir(), files_to_exclude, end_string)

    
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
                df = col_relabel(df, name)          
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
                
        df = spread_cols(df, df_name, attributes, 
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
        
        df = spread_cols(df, df_name, attributes, 
                         var_1='metadata.json.taskIdentifier',
                         var_2='data.json.variableLabel')

    elif df_name == 'demographics-v2' or df_name == 'generally_sem_diff_bl-v2':
        indices = df.index[df.externalId == 'yQ7pYy']
        df.drop(indices[-1], axis=0, inplace=True)

    elif df_name == 'behavior_choices_4_bl-v2':
        attributes = [
            'baseline'        
        ]
        
        df = spread_cols(df, df_name, attributes,
                         var_1='metadata.json.taskIdentifier')
        
    elif df_name == 'discounting_raw-v2':
        bl = 'baseline'
        _21 = '21-day-assessment'

        attributes = [
            [bl, 'pd_constant_money'],
            [bl, 'pd_constant_probabiliy'],
            [_21, 'pd_constant_money'],
            [_21, 'pd_constant_probability']
        ]
        
        df = spread_cols(df, df_name, attributes,
                         var_1='metadata.json.taskIdentifier',
                         var_2='data.json.variableLabel')
    
    elif df_name == 'evening_notification_time-v2':
        attributes = ['baseline']

        df = spread_cols(df, df_name, attributes,
                         var_1='metadata.json.taskIdentifier')
                        
    elif df_name == 'goNoGo-v2':
        bl = 'baseline'
        _21 = '21-day-assessment'

        attributes = [
            [bl, 'go_no_go_stable_stimulus_active_task'],
            [bl, 'go_no_go_variable_stimulus_active_task'],
            [_21, 'go_no_go_stable_stimulus_active_task'],
            [_21, 'go_no_go_variable_stimulus_active_task']
        ]
        
        indices = df.index[df.externalId == 'ksJM3Y']
        df.drop(indices[-1], axis=0, inplace=True)
        
        df = spread_cols(df, df_name, attributes,
                         var_1='metadata.json.taskIdentifier',
                         var_2='data.json.variable_label')
    
    elif df_name == 'morning_notification_time-v3':
        attributes = ['baseline']
        
        df = spread_cols(df, df_name, attributes,
                         var_1='metadata.json.taskIdentifier')
    
    elif df_name == 'pam_multiple-v2':
        attributes = [
            'baseline',
            '21-day-assessment'
        ]
        
        df = spread_cols(df, df_name, attributes,
                         var_1='metadata.json.taskIdentifier')        
        
    return df

    
def spread_cols(df, df_name, attributes, var_1, var_2=None):
    '''
    Merge the dataframes into one, with new columns.
    '''
    
    dfs = []
    
    for att in attributes:
        new_df = filter_df(df, att, var_1, var_2)
        new_df = col_relabel(new_df, df_name, att)
        dfs.append(new_df)
    
    new_df = reduce(lambda left, right: pd.merge(left, 
                                                 right, 
                                                 how = 'outer',
                                                 on='externalId'),
                    dfs)
    
    return new_df


def filter_df(df, att, var_1, var_2=None):
    '''
    Filter a dataframe based off of var_1 and var_2 variables.
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


def col_relabel(df, prefix, att=None):
    '''
    Remove: 'metadata.json.' and 'data.json.'.
    Replace spaces with underscores.
    Append new name to existing column name, except if externalId.
    '''
    new_cols = []
    col_re = re.compile('metadata.json.|data.json.')
    
    
    for col in df.columns:
        new_col = re.sub(col_re, '', col)

        if new_col != 'externalId':
            if att and len(att) == 2:
                new_col = '{}_{}_{}_{}___{}'.format('SAGE', 
                                                    prefix, 
                                                    att[0], 
                                                    att[1], 
                                                    new_col
                                                    )
            elif att and len(att) == 1:
                new_col = '{}_{}_{}___{}'.format('SAGE', 
                                                 prefix, 
                                                 att[0], 
                                                 new_col
                                                 )                
            
            else:
                new_col = '{}_{}___{}'.format('SAGE', 
                                              prefix, 
                                              new_col
                                              ) 
                                              
        new_col = new_col.replace(' ', '_')
        new_col = new_col.replace('-', '_')
        new_cols.append(new_col)
    
    df.columns = new_cols
    
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

test = main()

#def dupe_check(df):
#    return len(df.externalId) == len(df.externalId.unique()) and len(df.externalId) > 0


#test = merge_files(os.listdir(), files_to_exclude, end_string)





