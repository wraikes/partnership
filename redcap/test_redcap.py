
block1[block1['external_id'].isin(test_users + diff_study)][['internal_id', 'external_id']]




len(block_merge['internal_id']) == len(block_merge['internal_id'].unique()) and block_merge.shape[0] > 0


block_merge.shape
