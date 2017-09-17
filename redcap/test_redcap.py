import pandas as pd, unittest, os
from redcap import data_clean, column_relabel, remove_dupes, merge_data

redcap = '/home/wraikes/Dropbox/partnership/DMTdata_9_8_17/redcap/'
#redcap = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp'

class TestRedcap(unittest.TestCase):
    
    def setUp(self):
        os.chdir(redcap)
        
        self.block1 = pd.read_csv('REDCap Block 1_Merged_9.8.17.csv')
        self.block2 = pd.read_csv('REDCap Block 2_Merged_9.8.17.csv')
        
        self.dfs = [self.block1, self.block2]
        
        self.names = ['REDCAP_block1___', 'REDCAP_block2___']
        
        self.test_users = [
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

        self.diff_study = [
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
        
    def test_id_in_all_files(self):
        self.assertTrue(all(['internal_id' in x.columns for x in self.dfs]))
        self.assertTrue(all(['external_id' in x.columns for x in self.dfs]))
    
    def test_new_columns(self):
        for name, df in zip(self.names, self.dfs):
            new_cols = column_relabel(df, name, 'internal_id')
            self.assertEqual(len(new_cols) - 1, sum([x[0:len(name)] == name for x in new_cols]))
            
    def test_missing_internal_id(self):
        '''Test to see if there are internal_ids that match the external_ids. Block2 is missing some external_ids so
        internal_id may need to be used'''
        
        ids_remove = self.test_users + self.diff_study
        df_remove = self.block1[self.block1['external_id'].isin(ids_remove)][['internal_id', 'external_id']]
        self.assertEqual(0, df_remove.shape[0])
 
    def test_block1_duplicates(self):
        df = remove_dupes(self.block1)
        df = data_clean(df)
        full_ids = len(df['internal_id'])
        unique_ids = len(df['internal_id'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_block2_duplicates(self):
        df = remove_dupes(self.block2)
        df = data_clean(df)
        full_ids = len(df['internal_id'])
        unique_ids = len(df['internal_id'].unique())
        self.assertTrue(full_ids, unique_ids)
        
    def test_data_clean(self):
        '''external_id record "2XyZyJ" should be "2XyVyJ"
        '''
        for df in self.dfs:
            self.assertTrue(any(df.external_id == '2XyZyJ'))
            df_clean = data_clean(df)
            self.assertFalse(any(df_clean.external_id == '2XyZyJ'))
            self.assertTrue(any(df_clean.external_id == '2XyVyJ'))

    def test_redcap_merge_duplicates(self):
        clean_dfs = []
        
        for df in self.dfs:
            df = remove_dupes(df)
            df = data_clean(df)
            clean_dfs.append(df)
            
        final_data = merge_data(clean_dfs, self.names, 'internal_id')
        full_ids = len(final_data['internal_id'])
        unique_ids = len(final_data['internal_id'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_inquisit_merge_shape(self):
        clean_dfs = []
        
        for df in self.dfs:
            df = remove_dupes(df)
            df = data_clean(df)
            clean_dfs.append(df)
            
        final_data = merge_data(clean_dfs, self.names, 'internal_id')
        rows = final_data.shape[0]
        cols = final_data.shape[1]
        cols_total = sum([x.shape[1] for x in self.dfs]) - len(self.dfs) + 1
        self.assertTrue(rows > 0)
        self.assertEqual(cols_total, cols)






