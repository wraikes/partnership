import os, pandas as pd, numpy as np
from functools import reduce


def data_clean(df):
    wrong_ids = ['2XyZyJ', 'VytY4s', 'heVh5E', '4CUvwu']
    right_ids = ['2XyVyJ', 'VytY4S', 'hEVh5E', '4CUvwZ']
    
    for wrong, right in zip(wrong_ids, right_ids):
        if any(df['external_id'] == wrong):
            ix = df.index[df['external_id'] == wrong]
            df = df.set_value(ix, 'external_id', right)

    return df


def column_relabel(df, prefix, col_name):
    
    '''A function to relabel the columns
    with the df name appended to each 
    column name.'''
    
    new_cols = []
    for col in df.columns:
        if col != col_name:
            new_col = prefix + col
            new_col = new_col.replace(' ', '_')
            new_col = new_col.replace('-', '_')
            new_cols.append(new_col)
        else:
            new_cols.append(col)
    
    return new_cols


def merge_data(_dfs, _names, _id):
    new_dfs = []
    
    for df, name in zip(_dfs, _names):
        df = remove_dupes(df)
        df = data_clean(df)
        df.columns = column_relabel(df, name, _id)
        new_dfs.append(df)

    new_df = reduce(lambda left, right: pd.merge(left, right, how = 'outer',
                                                 on=_id), 
                                                 new_dfs)
    return new_df


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
        'ULoF3MM1nN',
        'G9Bbsg'
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
    
    return df[~df['external_id'].isin(test_users + diff_study)] 


def main():
    #NOTE TO SELF: redo the relative paths.
    redcap = '/home/wraikes/Dropbox/partnership/dmt/data/redcap_final/'
    os_file = '/home/wraikes/Programming/Partnership/dmt/final/merged_data/'
    #redcap = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt\data\redcap_final'
    #os_file = r'H:\Documents\Python Scripts\dmt\partnership\merged_data'
    os.chdir(redcap)  

    block1 = pd.read_csv('REDCap_Block 1_Merged_9.15.17.csv')
    block2 = pd.read_csv('REDCap_Block 2_Merged_9.15.17.csv')

    dfs = [block1, block2]
    names = ['REDCAP_block1___', 'REDCAP_block2___']    
    
    redcap_merge = merge_data(dfs, names, 'internal_id')
    os.chdir(os_file)
    redcap_merge.to_csv('FINAL_REDCAP.csv', index=False)

    
if __name__ == '__main__':
    main()
    


