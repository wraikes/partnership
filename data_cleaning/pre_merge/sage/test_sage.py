import pandas as pd, unittest, os, re, numpy as np
from sage import *


sage = '/home/wraikes/Dropbox/partnership/dmt/data/sage_final/'
#sage = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp\Sage (Mobile Phone) Data 8.31.17'


class TestSage(unittest.TestCase):
        
    def setUp(self):           
        os.chdir(sage)
        
        self.bart = pd.read_csv('digital-marshmallow-bart-v4.csv')
       
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

        self.files_to_exclude = [
                'digital-marshmallow-status.csv',
                'digital-marshmallow-appVersion.csv'
            ]
            
        self.end_string = '.csv'  
        
        
    def test_create_df_name(self):
        bart_string = 'digital-marshmallow-bart-v4.csv'
        string = create_df_name(bart_string, self.end_string)
            
        self.assertEqual('bart-v4', string)
        
        
    def test_col_relabel(self):
        attribute = ['21-day-assessment', 'BART0.25']
        new_df = col_relabel(self.bart, 'bart_v4', attribute)
        cols = new_df.columns
        col_string = 'SAGE_bart_v4_21_day_assessment_BART0.25'

        self.assertEqual(self.bart.shape[1] - 1, 
                         sum([x[0:len(col_string)] == col_string for x in cols]))        
        self.assertFalse(any(['-' in x for x in cols]))
        self.assertFalse(any([' ' in x for x in cols]))


    def test_filter_df(self):
        attributes = ['baseline', 'BART0.25']

        nrows = self.bart[
        (self.bart['metadata.json.taskIdentifier'] == attributes[0]) & 
        (self.bart['data.json.variable_label'] == attributes[1])
        ].shape[0]
        
        new_df = filter_df(self.bart, 
                           attributes,
                           var_1='metadata.json.taskIdentifier', 
                           var_2='data.json.variable_label'
                          )
        
        self.assertEqual(nrows, new_df.shape[0])
        
        
    def test_spread_cols_bart(self):
        attributes = [
            ['baseline', 'BART0.25'],
            ['baseline', 'BART250.00'],
            ['21-day-assessment', 'BART0.25'],
            ['21-day-assessment', 'BART250.00']
        ]
        
        bart_name = 'bart-v4'
        
        new_df = spread_cols(self.bart, bart_name, attributes, 
                             var_1='metadata.json.taskIdentifier', 
                             var_2='data.json.variable_label')
        
        self.assertEqual(self.bart.shape[1] * 4 - len(attributes) + 1, 
                         new_df.shape[1])        
        
    def test_spread_cols_delay(self):
        delay = pd.read_csv('digital-marshmallow-delay_discounting_raw-v6.csv')
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
        
        delay_name = 'delay'
        
        new_df = spread_cols(delay, delay_name, attributes, 
                             var_1='metadata.json.taskIdentifier', 
                             var_2='data.json.variableLabel')
        
        self.assertEqual(delay.shape[1] * 8 - len(attributes) + 1, 
                         new_df.shape[1])  
            
    #def test_col_relabel(self):
        #    bart = pd.read_csv('digital-marshmallow-bart-v4_8.31.17.csv')           
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            


