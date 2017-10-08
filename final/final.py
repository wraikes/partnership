import pandas as pd, numpy as np, os
from functools import reduce

def main():
    os.chdir('/home/wraikes/Programming/Partnership/dmt/final/merged_data/')
    #os.chdir(r'H:\Documents\Python Scripts\dmt\partnership\merged_data')

    inquisit = pd.read_csv('FINAL_INQUISIT.csv')
    redcap = pd.read_csv('FINAL_REDCAP.csv')
    sage = pd.read_csv('FINAL_SAGE.csv')

    final = pd.merge(sage, redcap, left_on='externalId', right_on='REDCAP_block1___external_id', how='outer')
    final = pd.merge(final, inquisit, left_on='internal_id', right_on='script.subjectid', how='outer')

    final.to_csv('DMT_DRAFT.csv', index=False)

    
if __name__ == '__main__':
    main()

