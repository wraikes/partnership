# -*- coding: utf-8 -*-
"""
Merge data files.
"""

import os, pandas as pd, numpy as np
from functools import reduce

inquisit = r'\\pdfk-dc\home\williamr\Desktop\DMTBilly data - Copy\Inquisit Data\Merged Data Sets'
redcap = r'\\pdfk-dc\home\williamr\Desktop\DMTBilly data - Copy\REDCap Data'
sage = r'\\pdfk-dc\home\williamr\Desktop\DMTBilly data - Copy\Sage Data'

#Inquisit Data Merge

os.chdir(inquisit)      
BART_Merged = pd.read_csv('BART_Merged_8.8.17.csv')
DelayDiscounting = pd.read_csv('DelayDiscounting_Merged_8.8.17.csv')
GoNoGo = pd.read_csv('GoNoGo_Merged_8.8.17.csv')
dfs = [BART_Merged, DelayDiscounting, GoNoGo]

#Check to see if script.subjectid (InternalID) is in Inquisit data
cols = [x.columns for x in dfs]
all(['script.subjectid' in list for list in cols])

#Drop extra blank rows in datafiles (not sure why xls to csv keeps them).
BART_Merged.dropna(axis = 0, how = 'all', inplace = True)
DelayDiscounting.dropna(axis = 0, how = 'all', inplace = True)
GoNoGo.dropna(axis = 0, how = 'all', inplace = True)

inquisit_merge = reduce(lambda left, right: pd.merge(left, right, how = 'outer',
                                                     on='script.subjectid'), 
                                                     dfs)

test_shape = max([x.shape[0] for x in dfs]), sum([x.shape[1] for x in dfs]) - 2
inquisit_merge.shape == test_shape


