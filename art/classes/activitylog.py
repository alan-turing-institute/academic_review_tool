from datetime import datetime

import pandas as pd
import numpy as np

class ActivityLog(pd.DataFrame):

    """
    This is an ActivityLog object. It is a modified Pandas Dataframe object designed to store metadata about an academic review.
    
    Columns
    -------
    * **timestamp**: date-time the activity occurred.
    * **type**: type of activity.
    * **activity**: details of activity.
    * **location**: location in Review that activity occurred.
    * **database**: name of database/repository accessed (if relevant).
    * **url**: web address accessed (if relevant).
    * **query**: search query used (if relevant).
    * **changes**: number of changes made to the Review results.
    """

    def __init__(self):
        
        """
        Initialises ActivityLog instance.
        """


        # Inheriting methods and attributes from Pandas.DataFrame class
        super().__init__(dtype=object, columns = [ # type: ignore
                                'timestamp',
                                'type',
                                'activity',
                                'location',
                                'database',
                                'url',
                                'query',
                                'changes'
                                ]
                         )
        
        self.replace(np.nan, None)
    

    def add_activity(self, type: str, activity: str, location: list, database = None, query = None, url = None, changes_dict = None):

        """
        Adds a new activity to the ActivityLog DataFrame.

        Parameters
        ----------
        type : str
            type of activity.
        activity : str
            details of activity.
        location : str
            name of location in Review that activity occurred.
        database : str
            name of database/repository accessed (if relevant). Defaults to None.
        query : str
            search query used (if relevant). Defaults to None.
        url : str
            web address accessed (if relevant). Defaults to None.
        changes_dict : dict
            dictionary of changes made to Review. Defaults to None.
        """

        new_index = len(self)
        self.loc[new_index, 'timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        self.loc[new_index, 'type'] = type
        self.loc[new_index, 'activity'] = activity
        self.at[new_index, 'location'] = location
        self.loc[new_index, 'database'] = database
        self.loc[new_index, 'url'] = url
        self.loc[new_index, 'query'] = query
        self.at[new_index, 'changes'] = changes_dict

