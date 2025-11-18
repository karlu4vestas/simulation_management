""" 
# The algo was commented out because it is slow and not required for LaC. 
# If ever to be used then limit the scan to extract the sid.
# To covert to owner initials. Build a thread safe lookup table to avoid multple conversion of the same sid
# also exclude initails that are not interesting to the user: service account, adm accounts etc and T* account

from concurrent.futures import ThreadPoolExecutor
from collections import Counter

import clr
import System
import System.IO
import System.Security.AccessControl    
from  System.Security.Principal import SecurityIdentifier, NTAccount
from System.Security.AccessControl import AccessControlSections    
from System import Type


class FileOwner:
    # class SidValueCount:
    #     def __init__(self,sid,owner, count=1):
    #         self.sid   = sid
    #         self.count = count
    #         self.owner = owner

    # @staticmethod
    # def getQueueSize(): return FileOwner.nbThreadPools

    # @staticmethod
    # def deleteQueue(): 
    #     FileOwner.threadpools = None

            
    #return dict of users and the number of files they own
    #  # path too long
    #  * path less than 260 but an exception happened in the call to System.IO.File.GetAccessControl or fileSecurity.GetOwner returned None 
    #  + path less than 260 but  the call to System.IO.File.GetAccessControl or fileSecurity.GetOwner returned None
    @staticmethod
    def getSid( path ):
        try:
            error = sid = "-"
            if len(path) <= 259:
                fileSecurity = System.IO.File.GetAccessControl(path, AccessControlSections.Owner)
                sid          = fileSecurity.GetOwner(SecurityIdentifier) if not (fileSecurity is None) else None
                if sid is None: 
                    error = sid = "+"
            else:
                error = sid = "#"    
        except Exception as e:
            error = sid = "-"
            pass

        return sid, error
    
    #get owner of each file
    @staticmethod
    def getOwners( entries, threadpool ):
        is_scan_path_UNC = params.scan_path[0:4]=="\\\\?\\"    
    
        # GetAccessControl cannot handle long paths unc format and paths longer than 256 characters 
        # so owners of longer filepaths must be ignored   
        if is_scan_path_UNC:
            paths = ["\\\\"+entry.path[8:] if entry.path[4:7]=="UNC" else entry.path[4:] for entry in entries]
        else:
            paths = [entry.path for entry in entries]
            
        
        sid_count =  dict() #use a dict to minize number of calls to sid.Translate(NTAccount).Value
        all_owners = []                                
        for sid,error in threadpool.map(FileOwner.getSid, paths ):   
            try:   
                str_sid = str(sid)         
                if str_sid=="*" or str_sid=="#" or str_sid=="+" or str_sid=="-" :
                    #The error can be:    
                    #   * stands for account no longer in vestas 
                    #   # stands for too long filename
                    #   + missing sid
                    #   - exception thrown
                    sid_count[str_sid] = error
                    owner = str_sid
                elif not str_sid in sid_count:
                    owner = sid.Translate(NTAccount).Value
                    if owner[0:7]=="VESTAS\\" : owner = owner[7:]       
                    sid_count[str_sid] = owner                          
                else:
                    owner = sid_count[str_sid]
                                                        
            except Exception as e:
                owner = error = sid = "*"
                sid_count[sid] = error
                pass

            all_owners.append(owner)
        
        return all_owners                     
"""