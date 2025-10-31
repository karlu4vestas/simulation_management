from __future__ import annotations #due to BaseSimulation in get_output_files 

KEEP  = "keep"
CLEAN = "clean"

class clean_folder_type:
    def __init__(self,  local_folder_names, extensions ):
        #the key is the l0 foldername in lower case
        self.key                = local_folder_names[0].lower() 
        self.local_folder_names = local_folder_names 
        self.extensions         = (*extensions,)

    #return the list of files to be deleted    
    def retrieve_file_list(self, simulation, base_path):
        raise NotImplementedError

class BaseSimulation :
    def __init__(self,base_path):
        self.base_path=base_path

    def hasValidSetNames(self): 
        raise NotImplementedError

    def getSetNames(self):  #return []
        raise NotImplementedError

    #returns copy of setname dict
    #def getSetNames_cpy(self):  #return []
    #    raise NotImplementedError

    def get_cleaner_files(self): #return {}
        raise NotImplementedError
