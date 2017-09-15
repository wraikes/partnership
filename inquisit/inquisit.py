import os, pandas as pd, numpy as np
from functools import reduce

#NOTE TO SELF: redo the relative paths.
inquisit = '/home/wraikes/Dropbox/partnership/DMTdata_9_8_17/inquisit/'
os_file = '/home/wraikes/Programming/Partnership/dmt/final/merged_data/'
#inquisit = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp'
#os_file = r'H:\Documents\Python Scripts\dmt\partnership\merged_data'

os.chdir(inquisit)  

bart = pd.read_csv('BART_Merged_9.8.17.csv')
delay1 = pd.read_csv('DelayDiscounting_Current_Merged_9.8.17.csv')
delay2 = pd.read_csv('DelayDiscounting_Original_Merged_9.8.17.csv')
delay3 = pd.read_csv('DelayDiscounting_v2_Merged_9.8.17.csv')
gonogo = pd.read_csv('GoNoGo_Merged_9.8.17.csv')

dfs = [bart, delay1, delay2, delay3, gonogo]

names = [
    'INQUISIT_bart___', 
    'INQUISIT_delay_current___', 
    'INQUISIT_delay_original___',
    'INQUISIT_delay_v2___',
    'INQUISIT_gonogo___'
        ]

def column_relabel(df, append_1, col_name):
    
    '''A function to relabel the columns
    with the df name appended to each 
    column name.'''
    
    new_cols = []
    for col in df.columns:
        if col != col_name:
            new_col = col.replace(' ', '_')
            new_col = append_1 + col
            new_cols.append(new_col)
        else:
            new_cols.append(col)
    
    return new_cols

def merge_data(_dfs, _names, _id):
    new_dfs = []
    
    for df, name in zip(_dfs, _names):
        df.columns = column_relabel(df, name, _id)
        new_dfs.append(df)

    new_df = reduce(lambda left, right: pd.merge(left, right, how = 'outer',
                                                 on=_id), 
                                                 new_dfs)
    return new_df



inquisit_merge = merge_data(dfs, names, 'script.subjectid')
os.chdir(os_file)
inquisit_merge.to_csv('FINAL_INQUISIT.csv', index=False)



