from datetime import datetime

import pandas as pd
import numpy as np

class ActivityLog(pd.DataFrame):

    """
    This is a ActivityLog object. It is a modified Pandas Dataframe object designed to store metadata about an academic review.
    
    Parameters
    ----------
    
    
    Attributes
    ----------
    """

    def __init__(self):
        
        """
        Initialises ActivityLog instance.
        
        Parameters
        ----------
        """


        # Inheriting methods and attributes from Pandas.DataFrame class
        super().__init__(dtype=object, columns = [ # type: ignore
                                'type',
                                'timestamp',
                                'database',
                                'query',
                                'changes'
                                ]
                         )
        
        self.replace(np.nan, None)
    

    def add_activity(self, type, database = None, query = None, changes_dict = None):

        new_index = len(self)
        self.loc[new_index, 'type'] = type
        self.loc[new_index, 'timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.loc[new_index, 'database'] = database
        self.loc[new_index, 'query'] = query
        self.loc[new_index, 'changes'] = changes_dict

