import pandas as pd, unittest, os
from sage import create_df_name


sage = '/home/wraikes/Dropbox/partnership/dmt/data/sage_not_final/'
#sage = r'C:\Users\williamr.PDFK\Dropbox\partnership\dmt_temp\Sage (Mobile Phone) Data 8.31.17'


class TestSage(unittest.TestCase):
        
    def setUp(self):           
        os.chdir(sage)
       
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
                'digital-marshmallow-status_8.31.17.csv',
                'digital-marshmallow-appVersion_8.31.17.csv'
            ]
            
        self.end_string = '_8.31.17.csv'  
        
        
    def test_create_df_name(self):
        bart_string = 'digital-marshmallow-bart-v4_8.31.17.csv'
        string = create_df_name(bart_string, self.end_string)
            
        self.assertEqual('bart-v4', string)
            
            
    #def test_col_relabel(self):
        #    bart = pd.read_csv('digital-marshmallow-bart-v4_8.31.17.csv')           
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            


