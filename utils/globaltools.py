"""Variables and functions for interacting with the global environment."""

def get_var_name_str(variable):
    
    """
    Returns the string name of a variable as defined in the global or local environment.
    """
    
    # Searching global variables dictionary for key-value pairs that are identical to the item
    try:
        for key in globals().keys():
            if globals()[key] == variable:
                return key
    
    # Searching local variables dictionary for key-value pairs that are identical to the item
    except:
        try:
            for key in locals().keys():
                if locals()[key] == variable:
                    return key
        except:
            raise KeyError('No variable name found')