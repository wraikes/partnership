import unittest, pandas as pd, os
from inquisit import column_relabel, merge_data	

inquisit = '/home/wraikes/Dropbox/partnership/dmt/data/inquisit_final/'
#inquisit = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp'


class TestInquisit(unittest.TestCase):
    def setUp(self):
        os.chdir(inquisit)
        
        self.bart = pd.read_csv('BART_Merged_9.15.17.csv')
        self.delay1 = pd.read_csv('DelayDiscounting_Current_Merged_9.15.17.csv')
        self.delay2 = pd.read_csv('DelayDiscounting_Original_Merged_9.8.17.csv')
        self.delay3 = pd.read_csv('DelayDiscounting_v2_Merged_9.8.17.csv')
        self.gonogo = pd.read_csv('GoNoGo_Merged_9.15.17.csv')

        self.dfs = [self.bart, self.delay1, self.delay2, self.delay3, self.gonogo]

        self.names = [
           'INQUISIT_bart___', 
           'INQUISIT_delay_current___', 
           'INQUISIT_delay_original___',
           'INQUISIT_delay_v2___',
           'INQUISIT_gonogo___'
        ]

    def test_id_in_all_files(self):
        self.assertTrue(all(['script.subjectid' in x.columns for x in self.dfs]))
    
    def test_new_columns(self):
        for name, df in zip(self.names, self.dfs):
            new_cols = column_relabel(df, name, 'script.subjectid')
            self.assertEqual(len(new_cols) - 1, sum([x[0:len(name)] == name for x in new_cols]))

    def test_bart_duplicates(self):
        full_ids = len(self.bart['script.subjectid'])
        unique_ids = len(self.bart['script.subjectid'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_delay1_duplicates(self):
        full_ids = len(self.delay1['script.subjectid'])
        unique_ids = len(self.delay1['script.subjectid'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_delay2_duplicates(self):
        full_ids = len(self.delay2['script.subjectid'])
        unique_ids = len(self.delay2['script.subjectid'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_delay3_duplicates(self):
        full_ids = len(self.delay3['script.subjectid'])
        unique_ids = len(self.delay3['script.subjectid'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_gonogo_duplicates(self):
        full_ids = len(self.gonogo['script.subjectid'])
        unique_ids = len(self.gonogo['script.subjectid'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_inquisit_merge_duplicates(self):
        final_data = merge_data(self.dfs, self.names, 'script.subjectid')
        full_ids = len(final_data['script.subjectid'])
        unique_ids = len(final_data['script.subjectid'].unique())
        self.assertTrue(full_ids, unique_ids)

    def test_inquisit_merge_shape(self):
        final_data = merge_data(self.dfs, self.names, 'script.subjectid')
        rows = final_data.shape[0]
        cols = final_data.shape[1]
        cols_total = sum([x.shape[1] for x in self.dfs]) - len(self.dfs) + 1
        self.assertTrue(rows > 0)
        self.assertEqual(cols_total, cols)

                         





















