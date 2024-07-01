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
                                'activity',
                                'site'
                                ]
                         )
        
        self.replace(np.nan, None)
