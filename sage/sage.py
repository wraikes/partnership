import os, pandas as pd, numpy as np, re
from functools import reduce

sage = '/home/wraikes/Dropbox/partnership/DMTdata_8_31_17/sage'
#sage = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp\Sage (Mobile Phone) Data 8.31.17'
os.chdir(sage)
files_to_exclude = ['digital-marshmallow-status_8.31.17.csv',
                    'digital-marshmallow-appVersion_8.31.17.csv']
end_string = '_8.8.17'

def merge_files(directoy, exclude, end_string):
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

    elif df_name == '':
        attributes = []
        
        df = df_merge()
    
    elif df_name == '':
        attributes = []
        
        df = df_merge()
    
    
    elif df_name == '':
        attributes = []
        
        df = df_merge()
    

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


test = merge_files(os.listdir(), files_to_exclude, end_string)










# In[75]:

demos.externalId[demos.externalId.duplicated()]


# In[76]:

demos.index[demos.externalId == 'yQ7pYy']


# In[77]:

demos.drop(91, axis=0, inplace=True)


# In[78]:

dupe_check(demos)


# In[79]:

demos = remove_dupes(demos)
demos = new_cols(demos, 'SAGE_demos___')


# In[80]:

final_check(demos, 'demos')


# In[81]:

gen = pd.read_csv('digital-marshmallow-generally_sem_diff_bl-v2_8.31.17.csv')
gen = remove_dupes(gen)

dupe_check(gen)


# In[82]:

gen.externalId[gen.externalId.duplicated()]


# In[83]:

gen.index[gen.externalId == 'yQ7pYy']


# In[84]:

gen.drop(95, axis=0, inplace=True)


# In[85]:

dupe_check(gen)


# In[86]:

gen = remove_dupes(gen)
gen = new_cols(gen, 'SAGE_generally_sem_bl___')


# In[87]:

final_check(gen, 'gen')


# In[88]:

len(new_dfs)


# ### DataFrame: Bart_V4

# In[89]:

bart_v4 = pd.read_csv('digital-marshmallow-bart-v4_8.31.17.csv')
bart_v4 = remove_dupes(bart_v4)

dupe_check(bart_v4)


# In[90]:

bart_attributes = [
    ['SAGE_bart_bl_0.25___', 'baseline', 'BART0.25'],
    ['SAGE_bart_bl_250___', 'baseline', 'BART250.00'],
    ['SAGE_bart_21_0.25___', '21-day-assessment', 'BART0.25'],
    ['SAGE_bart_21_250___', '21-day-assessment', 'BART250.00']
]


# In[91]:

bart_v4 = df_merge(bart_v4, 
                   bart_attributes,
                   var_1='metadata.json.taskIdentifier',
                   var_2='data.json.variable_label')


# In[92]:

final_check(bart_v4, 'bart_v4')


# ### DataFrame: Behavior_choices_4

# In[93]:

behavior_4 = pd.read_csv('digital-marshmallow-behavior_choices_4_bl-v2_8.31.17.csv')
behavior_4 = remove_dupes(behavior_4)

dupe_check(behavior_4)


# In[94]:

behavior_4_attributes = [
    ['SAGE_behavior_4_bl___', 'baseline'],
]


# In[95]:

behavior_4 = df_merge(behavior_4, 
                      behavior_4_attributes,
                      var_1='metadata.json.taskIdentifier')


# In[96]:

behavior_4.externalId[behavior_4.externalId.duplicated()]


# In[97]:

behavior_4.index[behavior_4.externalId == 'yQ7pYy']


# In[98]:

behavior_4.drop(73, axis=0, inplace=True)


# In[99]:

dupe_check(behavior_4)


# In[100]:

final_check(behavior_4, 'behave_4')


# ### DataFrame: Delay Discounting

# In[36]:

delay = pd.read_csv('digital-marshmallow-delay_discounting_raw-v6_8.31.17.csv')
delay = remove_dupes(delay)

dupe_check(delay)


# In[37]:

bl = 'baseline'
_21 = '21-day-assessment'

delay_attributes = [
    ['SAGE_delay_bl_time_6_month___', bl, 'dd_time_6_month'],
    ['SAGE_delay_bl_money_6_month___', bl, 'dd_money_6_month'],
    ['SAGE_delay_bl_money_1_month___', bl, 'dd_money_1_month'],
    ['SAGE_delay_bl_time_1_year___', bl, 'dd_time_1_year'],
    ['SAGE_delay_21_time_6_month___', _21, 'dd_time_6_month'],
    ['SAGE_delay_21_money_6_month___', _21, 'dd_money_6_month'],
    ['SAGE_delay_21_money_1_month___', _21, 'dd_money_1_month'],
    ['SAGE_delay_21_time_1_year___', _21, 'dd_time_1_year']
]


# In[38]:

delay = df_merge(delay, 
                 delay_attributes, 
                 var_1='metadata.json.taskIdentifier',
                 var_2='data.json.variableLabel')


# In[39]:

final_check(delay, 'delay')


# ### DataFrame: Discounting Raw

# In[40]:

discount = pd.read_csv('digital-marshmallow-discounting_raw-v2_8.31.17.csv')
discount = remove_dupes(discount)

dupe_check(discount)


# In[41]:

bl = 'baseline'
_21 = '21-day-assessment'

discount_attributes = [
    ['SAGE_discount_bl_money___', bl, 'pd_constant_money'],
    ['SAGE_discount_bl_prob___', bl, 'pd_constant_probabiliy'],
    ['SAGE_discount_21_money___', _21, 'pd_constant_money'],
    ['SAGE_discount_21_prob___', _21, 'pd_constant_probability']
]

discount = df_merge(discount, 
                    discount_attributes,
                    var_1='metadata.json.taskIdentifier',
                    var_2='data.json.variableLabel')


# In[42]:

final_check(discount, 'discount')


# ### DataFrame: Evening Notification

# In[43]:

evening_note = pd.read_csv('digital-marshmallow-evening_notification_time-v2_8.31.17.csv')
evening_note = remove_dupes(evening_note)

dupe_check(evening_note)


# In[44]:

evening_note_attributes = [
    ['SAGE_evening_note_bl___', 'baseline']
]

evening_note_bl = df_merge(evening_note, 
                           evening_note_attributes,
                           var_1='metadata.json.taskIdentifier')


# In[45]:

final_check(evening_note_bl, 'evening_note')


# ### DataFrame: GoNoGo

# In[46]:

gonogo = pd.read_csv('digital-marshmallow-goNoGo-v2_8.31.17.csv')
gonogo = remove_dupes(gonogo)

dupe_check(gonogo)


# #### Remove ksJM3Y until further notice.

# In[47]:

gonogo[gonogo.externalId == 'ksJM3Y']


# In[48]:

gonogo.drop([669], inplace=True)


# ##### ksJM3Y

# In[49]:

bl = 'baseline'
_21 = '21-day-assessment'

gonogo_attributes = [
    ['SAGE_gonogo_bl_stable___', bl, 'go_no_go_stable_stimulus_active_task'],
    ['SAGE_gonogo_21_variable___', bl, 'go_no_go_variable_stimulus_active_task'],
    ['SAGE_gonogo_bl_stable___', _21, 'go_no_go_stable_stimulus_active_task'],
    ['SAGE_gonogo_21_variable___', _21, 'go_no_go_variable_stimulus_active_task']
]

gonogo = df_merge(gonogo, 
                  gonogo_attributes,
                  var_1='metadata.json.taskIdentifier',
                  var_2='data.json.variable_label')


# In[50]:

final_check(gonogo, 'gonogo')


# ### DataFrame: Morning Notifications

# In[51]:

morning_note = pd.read_csv('digital-marshmallow-morning_notification_time-v3_8.31.17.csv')
morning_note = remove_dupes(morning_note)

dupe_check(morning_note)

morning_note_attributes = [
    ['SAGE_morning_note_bl___', 'baseline']
]

morning_note = df_merge(morning_note, 
                        morning_note_attributes,
                        var_1='metadata.json.taskIdentifier')

final_check(morning_note, 'morning_note')


# ### DataFrame: PAM Multiple

pam_mult = pd.read_csv('digital-marshmallow-pam_multiple-v2_8.31.17.csv')
pam_mult = remove_dupes(pam_mult)

dupe_check(pam_mult)


# In[55]:

pam_mult_attributes = [
    ['SAGE_pam_mult_bl___', 'baseline'],
    ['SAGE_pam_mult_21___', '21-day-assessment']
]

pam_mult = df_merge(pam_mult, 
                    pam_mult_attributes,
                    var_1='metadata.json.taskIdentifier')


# In[56]:

final_check(pam_mult, 'pam_mult')


# ### Final Merge of All Sage Data

# In[57]:

final_df = reduce(lambda left, right: pd.merge(left, right, how = 'outer',
                                               on='externalId'), 
                  new_dfs.values())


# In[58]:

dupe_check(final_df)


# In[59]:

final_df.shape


# In[60]:

os.chdir('/home/wraikes/Programming/Partnership/dmt/merged_data/')
#os.chdir(r'H:\Documents\Python Scripts\dmt\partnership\merged_data')
final_df.to_csv('FINAL_SAGE.csv')

