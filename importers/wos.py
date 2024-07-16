import metaknowledge as mk

def import_wos(file_path: str = 'request_input'):

    if file_path == 'request_input':
        file_path = input('File path: ')
    
    file_path = file_path.strip()

    RC = mk.RecordCollection(file_path)

    return RC