import os, pandas as pd, numpy as np, re, time
from functools import reduce


def main():
    sage = '/home/wraikes/Dropbox/partnership/dmt/data/sage_final'
    #sage = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt\data\sage_final'
    file_to_write = '/home/wraikes/Programming/Partnership/dmt/final/merged_data'
    #file_to_write = r'H:\Documents\Python Scripts\dmt\partnership\merged_data'
    os.chdir(sage)
    
    files_to_exclude = ['digital-marshmallow-status.csv',
                        'digital-marshmallow-appVersion.csv']
    end_string = '.csv'
    bl = 'baseline'
    _21 = '21-day-assessment'
    
    common_df = clean_common_dfs(os.listdir(), files_to_exclude, end_string)
    
    #To target the behavior_choices_1 df
    common_df = selected_recode(common_df)
        
    bart_att = [
        [bl, 'BART0.25'],
        [bl, 'BART250.00'],
        [_21, 'BART0.25'],
        [_21, 'BART250.00']       
    ]
    
    bart_df = clean_df('digital-marshmallow-bart-v4.csv',
                   end_string,
                   bart_att,
                   var_2='data.json.variable_label')
    
    delay_att = [
        [bl, 'dd_time_6_month'],
        [bl, 'dd_money_6_month'],
        [bl, 'dd_money_1_month'],
        [bl, 'dd_time_1_year'],
        [_21, 'dd_time_6_month'],
        [_21, 'dd_money_6_month'],
        [_21, 'dd_money_1_month'],
        [_21, 'dd_time_1_year']            
    ]
    
    delay_df = clean_df('digital-marshmallow-delay_discounting_raw-v6.csv',
                    end_string,
                    delay_att,
                    var_2='data.json.variableLabel')    
    
    delay_df = df_array_last_value(delay_df)
    delay_df = df_split_array(delay_df)
    
    behave_4_att = [
        [bl]
    ]
    
    behave_4_df = clean_df('digital-marshmallow-behavior_choices_4_bl-v2.csv',
                       end_string,
                       behave_4_att)
    
    behave_4_df = selected_spread(behave_4_df)
    
    discount_att = [
        [bl, 'pd_constant_money'],
        [bl, 'pd_constant_probability'],
        [_21, 'pd_constant_money'],
        [_21, 'pd_constant_probability']
    ]
    
    discount_df = clean_df('digital-marshmallow-discounting_raw-v2.csv',
                      end_string,
                      discount_att,
                      var_2='data.json.variableLabel')
    
    evening_note_att = [
        [bl]
    ] 
    
    evening_note_df = clean_df('digital-marshmallow-evening_notification_time-v2.csv', 
                           end_string,
                           evening_note_att) 
    
    gonogo_att = [
        [bl, 'go_no_go_stable_stimulus_active_task'],
        [bl, 'go_no_go_variable_stimulus_active_task'],
        [_21, 'go_no_go_stable_stimulus_active_task'],
        [_21, 'go_no_go_variable_stimulus_active_task']
    ]
    
    gonogo_df = clean_df('digital-marshmallow-goNoGo-v2.csv', 
                     end_string,
                     gonogo_att,
                     var_2='data.json.variable_label')
    
    morning_note_att = [
        [bl]
    ]   
    
    morning_note_df = clean_df('digital-marshmallow-morning_notification_time-v3.csv',
                          end_string,
                          morning_note_att)
    
    pam_mult_att = [
        [bl],
        [_21]
    ]
     
    pam_mult_df = clean_df('digital-marshmallow-pam_multiple-v2.csv',
                      end_string,
                      pam_mult_att)
    
    df = merge_dfs([common_df, 
                        bart_df,
                        delay_df,
                        behave_4_df,
                        discount_df,
                        evening_note_df,
                        gonogo_df,
                        morning_note_df,
                        pam_mult_df])  

    df = timestamp_conv(df)
    
    os.chdir(file_to_write)
    df.to_csv('FINAL_SAGE.csv', index=False)


def merge_dfs(dfs):
    df = reduce(lambda left, right: pd.merge(left, right, how = 'outer',
                                             on='externalId'), 
                dfs)
    
    return df


def clean_common_dfs(directory, exclude_files, string):
    '''
    Go through and process all files in directory that don't need columns "spread".
    If extra processing is needed, a different function will be used.
    Return a single dataframe, of all data merged.
    '''
    
    new_dfs = []
    
    for file in directory: 
        if file not in exclude_files:
            df_name = create_df_name(file, string)
            df = pd.read_csv(file)
            df = remove_dupes(df)
            if df_name in ('demographics-v2', 'generally_sem_diff_bl-v2'):
                df = clean_df_dupe(df, 'yQ7pYy', -1)
            
            if len(df.externalId) == len(df.externalId.unique()):
                df = col_relabel(df, df_name)
                new_dfs.append(df)
    
    return merge_dfs(new_dfs)


def create_df_name(string, end_string):
    string = string.replace('digital-marshmallow-', '')
    string = string.replace(end_string, '')
    
    return string 


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
                new_col = 'SAGE_{}_{}_{}___{}'.format(prefix, 
                                                      att[0], 
                                                      att[1], 
                                                      new_col
                                                     )
            elif att and len(att) == 1:
                new_col = 'SAGE_{}_{}___{}'.format(prefix, 
                                                   att[0], 
                                                   new_col
                                                  )                
            
            else:
                new_col = 'SAGE_{}___{}'.format(prefix, 
                                                new_col
                                                ) 
                                              
        new_col = new_col.replace(' ', '_')
        new_col = new_col.replace('-', '_')
        new_cols.append(new_col)
    
    df.columns = new_cols
    
    return df


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
  
    return df[~df.externalId.isin(test_users + diff_study)]


def spread_cols(df, df_name, attributes, var_2):
    '''
    Merge the dataframes into one, with new columns.
    '''
    dfs = []
    var_1 = 'metadata.json.taskIdentifier'
    
    for att in attributes:
        cols = df.columns
        new_df = pd.DataFrame(columns=cols)
        
        for ix, row in df.iterrows():
            if var_2:
                if row[var_1] == att[0] and row[var_2] == att[1]:
                    new_df = new_df.append(row, ignore_index=True)
            else:
                if row[var_1] == att[0]:
                    new_df = new_df.append(row, ignore_index=True)
                    
        new_df = col_relabel(new_df, df_name, att)
        dfs.append(new_df)
    
    return merge_dfs(dfs)


def clean_df(file, string, attributes, var_2=None):
    df_name, df = create_df_name(file, string), pd.read_csv(file)
    df = remove_dupes(df)
    
    if df_name == 'goNoGo-v2':
        df = clean_df_dupe(df, 'ksJM3Y', -1)
        df = clean_df_dupe(df, 'S6b4eL', 11)
    
    if df_name == 'delay_discounting_raw-v6':
        df = clean_df_dupe(df, 'S6b4eL', 2)
        
    if df_name == 'discounting_raw-v2':
        df = clean_df_dupe(df, 'S6b4eL', 5)
    
    if df_name == 'behavior_choices_4_bl-v2':
        df = clean_df_dupe(df, 'yQ7pYy', -1)
    
    df = spread_cols(df, df_name, attributes, var_2)
    
    return df


def clean_df_dupe(df, _id, pos):
   
    indices = df.index[df.externalId == _id]
    df.drop(indices[pos], axis=0, inplace=True)
    
    return df


def timestamp_conv(df):
    
    for col in df.columns:
        if any([col.endswith(x) for x in ('createdOn', 'startDate', 'endDate')]):
            df[col] = df[col][df[col].notnull()].apply(lambda x: time.strftime("%m.%d.%Y %H:%M - %Z", time.localtime(x / 1000)))
    
    return df


def array_split(value, i):
    if pd.notnull(value):
        return list(map(float, value[1:-1].split(',')))[i]
    else:
        return value

    
def df_array_last_value(df):
    new_df = df.copy()

    for col in df.columns:
        if 'nowArray' in col:
            new_col = col + '_last'
            new_df[new_col] = df[col].apply(lambda x: array_split(x, -1))
    
    return new_df


def df_split_array(df):
    new_df = df.copy()
    
    for col in df.columns:
        if all([x in col for x in ('times', 'delay', 'SAGE')]):
            for i in range(6):
                new_col = col + '_v' + str(i)
                new_df[new_col] = df[col].apply(lambda x: array_split(x, i))
                
    return new_df


def selected_spread(df):
    #Note: codebook is incorrect. 'gamble' vs. 'gambling'.
    terms = ['junkfood', 'gambling', 'smoke', 'mobile', 'overeat', 'mast', 'cut', 'smedia',
             'alcohol', 'porn', 'hair', 'gaming', 'sitting', 'spend', 'temper', 'othermore']

    for term in terms:
        col_name = 'SAGE_behavior_choices_4_bl_v2_baseline___selected' + '_' + term
        df[col_name] = np.where(df['SAGE_behavior_choices_4_bl_v2_baseline___selected'].str.contains(term), 1, 0)
        
    return df


def recode(value):
    
    terms = ['junkfood', 'gambling', 'smoke', 'mobile', 'overeat', 'mast', 'cut', 'smedia',
             'alcohol', 'porn', 'hair', 'gaming', 'sitting', 'spend', 'temper', 'othermore']
    
    for ix, term in enumerate(terms):
        if isinstance(value, str) and term in value:
            return ix 

        
def selected_recode(df):
    
    df['SAGE_behavior_choices_1_bl_v1___selected_recode'] = df['SAGE_behavior_choices_1_bl_v1___selected'].apply(lambda x:\
                                                            recode(x) if pd.notnull(x) else x)
    
    return df


if __name__ == '__main__':
    main()
    print('Done!')

