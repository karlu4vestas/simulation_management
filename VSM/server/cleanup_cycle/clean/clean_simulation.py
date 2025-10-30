import os
import io
import re
import sys
import time
import numpy as np
import datetime
from enum import Enum
from collections import deque
from clean_folder_type import BaseSimulation, clean_folder_type
from clean_all_pr_ext import clean_all_pr_ext
from clean_all_but_one_pr_ext import clean_all_but_one_pr_ext  
from clean_parameters import clean_parameters
from clean_writer_tasks import FolderLogInfo, SimulationStatus, FileDeletionMethod

def format_time(t, default_value=None, date_only=False): 
    ft = default_value
    try:
        if not t is None:
            if date_only:
                ft = time.strftime("%Y-%m-%d", time.localtime(t))
            else:    
                ft = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t))
    except:
        ft = default_value

    return ft

class FolderType(Enum):
    MissingFolder      = "missing_folder"
    NormalFolder       = "normal_folder" #has only one or no standard folder
    PartialSimulation  = "partial_simulation" #missing the INPUTS folder
    StandardSimulation = "standard_simulation" #has all the standard folders
    StandardSimulation_partial = "standard_simulation_partial" #has the INPUTS folder and at lease one more standard folder
    def __str__(self):
        return self.name

class Simulation(BaseSimulation):
    cleaners:list[clean_folder_type] = [ 
                    clean_all_pr_ext(        ["INT"],          [".int",".tff"]),
                    clean_all_pr_ext(        ["EXTFND","dat"], [".sim"]),
                    clean_all_but_one_pr_ext(["EIG"],          [".eig", ".mtx"]),
                    clean_all_but_one_pr_ext(["OUT"],          [".out"]),
                    clean_all_but_one_pr_ext(["STA"],          [".sta"]),
                    clean_all_but_one_pr_ext(["LOG"],          [".log"]),

                    #add cleaners for hawc2 folders. Need to find the appropriate extensions for the below
                    #need to add the contend of the MASTER folder to the input files
                    #clean_all_pr_ext(        ["HTC"],           [".htc"]),
                    #in some simulations the WIND folder is located in the simulation while other are located outside the simulation ???
                    #clean_all_pr_ext(        ["WIND"],          [".bin"]), 
                    #clean_all_pr_ext(        ["HTCFILES"],      [".htc"]),
                    #clean_all_pr_ext(        ["RES"],           [".txt"]),
                    #clean_all_but_one_pr_ext(["LOGFILES"],      [".log"])

                ]   
    prepspace4_files2ignore= re.compile( r"^c1run.*\.bat|^c2run.*\.bat|\.onetoc2$|\.frq$|\.bak$|~$" +
                                         r"|^intpostdlog\.log|^femmodel\.ans|^success\.txt|^error\.txt|^int_file_status\.txt" +
                                         r"|.+resubmission\.bat|.+onesim\.bat|^rwpost.bat|^cwplot\.bat" +
                                         r"|^runsp\.bat|^runjobb\.bat|^distexe\.bat|^cmaster\.bat|^cdlltxt\.bat" +
                                         r"|^notfound.bat|^run.bat|^flxctrl_distributed.bat|^runmissingint.bat|^rwplot.bat|^missing_simulations_run.bat|^deletemindstorm.*\.bat|^launchmindstorm.*\.bat"
                                        ,re.IGNORECASE)

    def __init__(self, base_path, threadpool):
        self.base_path   = base_path
        self.simulation_status = None      
        self.base_entries = None
        self.direct_child_folders  = None
        self.standard_dirs = None
        self.folder_type = None
        self.set_names   = None
        self.set_extension_files = None
        self.input_files = None
        self.all_dir_entries = None
        self.all_file_entries = None
        self.set_output_files = None
        self.all_files_from_cleaner_folders = None
        self.cleaner_files_from_setnames = None
        self.prepper = None
        self.threadpool = threadpool

    def get_base_entries(self):
        if self.base_entries is None:
            try: 
                with os.scandir(self.base_path) as entries:
                    self.base_entries = [e for e in entries]
            except:
                self.base_entries = None
           
        return self.base_entries

    def get_direct_child_folders(self):
        if self.direct_child_folders is None:
            try: 
                if not self.get_base_entries() is None:
                    self.direct_child_folders = [e for e in self.get_base_entries() if e.is_dir(follow_symlinks=False) ] 
            except:
                self.direct_child_folders = None
           
        return self.direct_child_folders

    def get_standard_folders(self):
        if self.standard_dirs is None:
            params = clean_parameters.params
            try:
                if not self.get_direct_child_folders() is None:    
                    self.standard_dirs = [e for e in self.get_direct_child_folders() if e.name.casefold() in params.vts_standard_folders ] 
            except:
                self.standard_dirs = None
           
        return self.standard_dirs

    #can we continue with clean-up according to exclusion criteria
    def getFolderType(self):
        if self.folder_type is None:
            standard_dirs = self.get_standard_folders()
            
            #tjek that all standard folders er present
            if standard_dirs is None:
                self.folder_type = FolderType.MissingFolder
            elif len(standard_dirs) <= 1:
                self.folder_type = FolderType.NormalFolder     
            elif len(standard_dirs) < len(clean_parameters.params.vts_standard_folders):
                if len(list(filter(lambda e: "inputs" == e.name.casefold(), standard_dirs)))>0 :
                    self.folder_type = FolderType.StandardSimulation_partial
                else:    
                    self.folder_type = FolderType.PartialSimulation
            elif len(standard_dirs) == len(clean_parameters.params.vts_standard_folders):
                self.folder_type = FolderType.StandardSimulation
            else:
                print("SHOULD NOT HAPPEN: len(standard_dirs) > len(clean_parameters.params.simulation_standard_folders)" )
                self.folder_type = FolderType.MissingFolder
        return self.folder_type


    #-------------read all entries in the simulation' subtree and cache them so that no other functions (except get_standard_folders) needs to use scandir-----------
    def get_all_entries(self) :
        params = clean_parameters.params
        set_base_dirs, dir_queue = set(), []

        def scan( path ):
            entries_list = None
            max_failure, failures, succes = 2, 0, False
            while failures < max_failure and not succes:
                try:
                    entries_list:list[os.DirEntry] = []
                    with os.scandir(path) as entries_list:
                        entries_list:list[os.DirEntry] = [e for e in entries_list]
                    local_path                        = path[len(self.base_path):].strip("\\").lower()
                    self.all_file_entries[local_path] = [ e for e in entries_list if e.is_file(follow_symlinks=False) ]
                    self.all_dir_entries[local_path]  = [ e for e in entries_list if e.is_dir(follow_symlinks=False)  ]

                    dir_queue.extend( [e.path for e in self.all_dir_entries[local_path] if e.path.lower() not in set_base_dirs ] )             
                    #if path[-6:].lower()=="inputs":setnames = self.getSetNames()
                    #elif path[-4:].lower()=="prog":prepper = self.getPrepper()
                    #print(f"got inputs:{path} with number of setnames:{len(setnames)}")
                #except:
                #    pass      
                    succes = True
                except Exception as e: 
                    failures = failures + 1
                    if failures < max_failure:
                        time.sleep(0,5)
                    else:
                        if sys.exc_info() is None:
                            error_message = f"\"{path}\";\"{str(e)}\";\"\";\"\";\"\"\n"
                        else:    
                            exc_type, exc_value, exc_traceback = sys.exc_info()              
                            error_message = f"\"{path}\";\"{exc_type}\";\"{exc_value}\";\"{exc_traceback.tb_frame.f_code.co_name}\";\"{exc_traceback.tb_lineno}\"\n"
                        params.folder_error_queue.put(error_message)            

            return entries_list, path
        

        if self.all_dir_entries is None and (self.getFolderType() == FolderType.StandardSimulation or self.getFolderType() == FolderType.StandardSimulation_partial):             
            self.all_file_entries = {}
            self.all_dir_entries = {}

            #assume the standards folders are there so that we can request data about them from the beginning 
            base_dirs = [ self.base_path.lower()] 
            base_dirs.extend( [ e.path.lower() for e in self.get_base_entries() if e.is_dir(follow_symlinks=False) ] )
            set_base_dirs.update(base_dirs)
            dir_queue.extend( base_dirs )

            while len(dir_queue):
                paths = dir_queue.copy()
                dir_queue.clear()
                for _, _ in self.threadpool.map( scan, paths) :
                    continue
        elif self.all_dir_entries is None :
            self.all_file_entries = {}
            self.all_dir_entries = {}
            local_path = ""
            self.all_file_entries[local_path] = [ e for e in self.get_base_entries() if e.is_file(follow_symlinks=False) ]
            self.all_dir_entries[local_path]  = [ e for e in self.get_base_entries() if e.is_dir(follow_symlinks=False)  ]

                        
        return self.all_dir_entries, self.all_file_entries 

    def get_entries(self, local_path) :
        local_path = local_path.lower()
        all_dir_entries, all_file_entries = self.get_all_entries() 
        return all_dir_entries.get( local_path, [] ),  all_file_entries.get( local_path, [] )

    #-------------------------------
    #get all files matching the cleaners extensions
    def get_set_output_files_for_cleaners(self):
        if self.set_output_files is None:
            self.set_output_files = {}
            set_names = set(self.getSetNames())
            for cleaner in Simulation.cleaners:
                _, file_entries = self.get_entries( os.path.join(*cleaner.local_folder_names) )
                self.set_output_files[cleaner.key] = [ f for f in file_entries if f.name.lower().endswith( cleaner.extensions ) and (f.name.lower().rsplit(".",1)[0] in set_names) ]

        return self.set_output_files  

    #-------------------------------
    #For each cleaner. Only include 
    # - files with that match the cleaners extensions and files with names from the .set file
    # - for phase 2 files we also to not delete the file setname.ext where the setname is the first setname from the .set that is also found in the phase2 folder
    def get_cleaner_files(self):
        if self.cleaner_files_from_setnames is None:
            self.cleaner_files_from_setnames =  {}

            for cleaner in Simulation.cleaners:
                self.cleaner_files_from_setnames[cleaner.key] = cleaner.retrieve_file_list(self, self.base_path ) 

        return self.cleaner_files_from_setnames
    
    #---------------------------------
    #return the input files after applying the prepspace filter
    def getINPUTS_files(self):
        if self.input_files is None:
            _, file_entries  = self.get_entries("INPUTS")
            _, file_entries_MASTER  = self.get_entries("MASTER") #some hawc2 simulations have an input file in the MASTER folder
            file_entries.extend(file_entries_MASTER)
            if not file_entries is None:
                self.input_files = [f for f in file_entries if None == Simulation.prepspace4_files2ignore.search(f.name)]

        return self.input_files

    #-------------------------------
    def get_size_stat_from_file_entries(file_entries:dict):
        
        max_time = float(2**31 - 1) # last validt timestamp https://en.wikipedia.org/wiki/Year_2038_problem

        bytes_, count_files, max_date, min_date, file_entries =  0, 0, None, None, file_entries
        
        files_counts = [len(entries) for entries in file_entries.values() ]
        count_files  = sum(files_counts)
        files_stat   = [file.stat() for entries in file_entries.values() for file in entries ]
        count_files  = len(files_stat)
        if count_files > 0:
            file_bytes     = np.fromiter( (state.st_size  for state in files_stat), dtype=np.int64   ) 
            bytes_         = np.sum( file_bytes )

            file_timestamp = [state.st_mtime for state in files_stat if state.st_mtime < max_time] #have to create a list with valid timestamps first
            if len(file_timestamp) > 0: 
                file_timestamp = np.fromiter( file_timestamp, dtype=np.float64 )
                max_date       = np.max(file_timestamp) 
                min_date       = np.min(file_timestamp)

        return bytes_, count_files, max_date, min_date, file_entries

    def get_simulation_size(self):
        _, all_files = self.get_all_entries() 
        return Simulation.get_size_stat_from_file_entries(all_files)
    
    def get_cleaner_size(self):
        files = self.get_cleaner_files()
        return Simulation.get_size_stat_from_file_entries(files)

    #---------------------------
    # if setnames are invalid then the reproducibiliy must be False
    # if hasValidSetNames is Tue then procedd to evaluate reproducibility by comparing the youngest inout file to the oldest vts_output file
    # vts_output file are those matchin the cleaners extension
    def is_reproducible(self):    
        reproducible, max_INPUTS_time, min_set_output_time, desc_input_violations = False, None, None, ""
        try:
            _, _, max_INPUTS_time, _, input_file_entries = Simulation.get_size_stat_from_file_entries( {"inputs":self.getINPUTS_files()} )

            #reproducibiity require valid set names and is evaluated from the input files max datetime and min datetime of files matching the cleaners extensions
            if not self.hasValidSetNames(): 
                desc_input_violations   = f"invalid setfiles_count: {len(self.getSetFiles())}"
            elif max_INPUTS_time is None:
                desc_input_violations   = f"max_INPUTS_time is None"
            else:            
                #_, _, _, vts_output_min_date, vts_output_file_entries = self.get_vts_output_size()   
                _, _, _, min_set_output_time, _ = Simulation.get_size_stat_from_file_entries( self.get_set_output_files_for_cleaners() )   
                

                # if there is no outputs file then the reproducible=True because
                #  - either simulation is was not run yet or it was cleaned by removing all vts output which indicate reproducibility
                if not min_set_output_time is None:
                    reproducible = max_INPUTS_time < min_set_output_time
                else:
                    reproducible = True
                
                if not reproducible and not min_set_output_time is None and not max_INPUTS_time is None:
                    #if reproducibility is False then extract all input files younger than min_outputs_date, sorted fra youngest to oldest
                    input_file_entries      = input_file_entries["inputs"]
                    input_violation_entries = [ e for e in input_file_entries if e.stat().st_mtime >= min_set_output_time ]
                    ix_sort                 = np.argsort( np.fromiter( (e.stat().st_mtime for e in input_violation_entries), dtype=np.int64   ) )[::-1]
                    input_violation_entries = [ input_violation_entries[i] for i in ix_sort.tolist() ] 
                    desc_input_violations   = ";\n".join( [e.name for e in input_violation_entries] )
                
        except Exception as e:
            if sys.exc_info() is None:
                error_message = f"\"{str(e)}\";\"{self.base_path}\";\"\";\"\";\"\"\n"
            else:    
                exc_type, exc_value, exc_traceback = sys.exc_info()              
                error_message = f"\"{str(exc_type)}\";\"{self.base_path}\";\"{str(exc_value)}\";\"{str(exc_traceback.tb_frame.f_code.co_name)}\";\"{str(exc_traceback.tb_lineno)}\"\n"
            clean_parameters.params.folder_error_queue.put(error_message)
            print(error_message)

        return  bool(reproducible), max_INPUTS_time, min_set_output_time, desc_input_violations
 

    def getSetFiles(self): 
        if self.set_extension_files is None:
            self.set_extension_files = [e for e in self.getINPUTS_files() if ".set" == e.name[-4:].lower()]
        return self.set_extension_files
    
    def getSetNames(self):
        max_failures, failures = 2, 0
        succes = False
        while failures < max_failures and succes is False:
            try:
                if self.set_names is None:
                    set_files = self.getSetFiles()
                    self.set_names = []

                    if len(set_files)==1 :
                        with io.open( set_files[0].path, "r", encoding="utf-8", errors='ignore', buffering= 2**19) as file:
                            set_names_splitted_lines = re.sub("(>>\\s*\\n)"," ",file.read() ).lower().splitlines()

                        #split lines and skip the first 6. Then split each line to locate the setnames. 
                        #len(s) ensures we avoid empty lines
                        if len(set_names_splitted_lines) > 6:
                            set_names_splitted_lines = [s.split(None,1) for s in set_names_splitted_lines[6:]]
                            self.set_names = [s[0] for s in set_names_splitted_lines if len(s) > 0 ]

                succes = True
            except Exception as e:    
                failures = failures + 1
                if failures < max_failures:
                    time.sleep(1)
                else:
                    clean_parameters.params.folder_error_queue.put(f"\"getSetNames:{str(e)}\";\"{self.base_path}\"\n")

        return self.set_names

    def getPrepper(self):
        max_failures, failures = 2, 0
        succes = False
        while failures < max_failures and succes is False:
            try:
                if self.prepper is None:
                    self.prepper = ""    
                    _, file_entries  = self.get_entries("PROG")
                    prog_files = [e for e in file_entries if "prep-id.txt" == e.name.lower()]

                    if len(prog_files)==1 :
                        with io.open( prog_files[0].path, "r", encoding="utf-8", errors='ignore', buffering= 2**18) as file:
                            prep_lines = re.sub("(>>\\s*\\n)"," ",file.read() ).lower().splitlines()
                        
                        if len(prep_lines) > 1:
                            #create a dictionary and get the user
                            prep_lines = [s.split(":") for s in prep_lines[1:]]
                            prep_dic = {l[0]:l[1] for l in prep_lines if len(l)>=2 }
                            self.prepper = prep_dic.get("user","")

                succes = True
            except Exception as e:
                failures = failures + 1
                if failures < max_failures:
                    time.sleep(1)
                else:
                    clean_parameters.params.folder_error_queue.put(f"\"getPrepper: {str(e)}\";\"{self.base_path}\"\n")

        return self.prepper

    #---------------------------- evaluations follows belows -------------------------------
    def hasValidSetNames(self): 
        setnames =  self.getSetNames()
        return len(setnames) > 0 

    def get_folder_exclusions(self, max_modification_date_in_simulation):
        #optimize by joining all paths so that on regex search is sufficient 
        params   = clean_parameters.params
        dirs, _  = self.get_entries("")
        all_text = "|".join( [self.base_path, *[e.name for e in dirs ] ])
        matches  = "|".join( [m.group(0) for m in clean_parameters.params.re_folder_exclusions.finditer(all_text)] )
        
        if len(matches) == 0 :
            if not params.min_date is None and not params.max_date is None and not max_modification_date_in_simulation is None:
                in_datetime_scope =  max_modification_date_in_simulation >= params.min_date and max_modification_date_in_simulation < params.max_date 
                if not in_datetime_scope:
                    matches = f"sim_max_date before {params.min_date.isoformat(' ')}" if max_modification_date_in_simulation < params.min_date else f"sim_max_date after {params.max_date.isoformat(' ')}"
        
        return len(matches)==0, matches

    #@todo convert to date
    #introduce incomplet simulation
    def eval(self, file_deletion_method):
        all_cleaners_files=None
        try:
            if self.simulation_status is None:
                self.simulation_status = SimulationStatus(self.base_path)         

                folder_type                                                  = self.getFolderType()
                sim_bytes, sim_count_files, sim_max_datetime, _, _           = self.get_simulation_size()
                self.simulation_status.add( FolderLogInfo.folder_type,             None,        str( folder_type ) )
                self.simulation_status.add( FolderLogInfo.folder_files,            None,        str( sim_count_files)       )
                self.simulation_status.add( FolderLogInfo.folder_bytes,            None,        str( sim_bytes)             )
                self.simulation_status.add( FolderLogInfo.folder_max_date,         None,        format_time(sim_max_datetime, default_value="", date_only=True)  )

                if folder_type == FolderType.StandardSimulation or folder_type==FolderType.StandardSimulation_partial: 
                    all_files_from_cleaner_folders = self.get_set_output_files_for_cleaners()
                    #@todo rewrite to include all registered cleaners 
                    int_files      = all_files_from_cleaner_folders["int"]
                    extfnd_files   = all_files_from_cleaner_folders["extfnd"]
                    eig_files      = all_files_from_cleaner_folders["eig"]
                    out_files      = all_files_from_cleaner_folders["out"]
                    log_files      = all_files_from_cleaner_folders["log"]
                    sta_files      = all_files_from_cleaner_folders["sta"]

                    htc_files      = all_files_from_cleaner_folders["htc"]
                    htcFILES_files = all_files_from_cleaner_folders["htcfiles"]
                    wind_files     = all_files_from_cleaner_folders["wind"]
                    res_files      = all_files_from_cleaner_folders["res"]
                    logfiles_files = all_files_from_cleaner_folders["logfiles"]

                    #@todo convert to date
                    sim_max_datetime                                             = datetime.datetime.fromtimestamp(sim_max_datetime) if not sim_max_datetime is None else None
                    isNotExcluded_, str_exclusions                               = self.get_folder_exclusions(sim_max_datetime)

                    len_set_names                                                = len(self.getSetNames())
                    has_valid_setNames                                           = len_set_names > 0

                    is_reproducible, max_INPUTS_time, min_set_output_time, reproducibility_violations = self.is_reproducible()
                    str_reproducibility_status =   "irreproducible"
                    if not has_valid_setNames:
                        str_reproducibility_status = "irreproducible due to setfiles"
                    elif is_reproducible:
                        str_reproducibility_status = "reproducible"

                    self.simulation_status.add( FolderLogInfo.prepper,              None,        self.getPrepper()                     )
                    self.simulation_status.add( FolderLogInfo.setname_count,        None,        str( len_set_names ) if len_set_names>0 else "-1"   )

                    def na_len(files): return "NA" if files is None else str( len(files) )
                    self.simulation_status.add( FolderLogInfo.int_file_count,    None, na_len( int_files    ) )
                    self.simulation_status.add( FolderLogInfo.extfnd_file_count, None, na_len( extfnd_files ) )
                    self.simulation_status.add( FolderLogInfo.eig_file_count,    None, na_len( eig_files    ) )
                    self.simulation_status.add( FolderLogInfo.out_file_count,    None, na_len( out_files    ) )
                    self.simulation_status.add( FolderLogInfo.log_file_count,    None, na_len( log_files    ) )
                    self.simulation_status.add( FolderLogInfo.sta_file_count,    None, na_len( sta_files    ) )

                    self.simulation_status.add( FolderLogInfo.htc_file_count,      None, na_len( htc_files     ) )
                    self.simulation_status.add( FolderLogInfo.htcfiles_file_count, None, na_len( htcFILES_files) )
                    self.simulation_status.add( FolderLogInfo.wind_file_count,     None, na_len( wind_files    ) )
                    self.simulation_status.add( FolderLogInfo.res_file_count,      None, na_len( res_files     ) )
                    self.simulation_status.add( FolderLogInfo.logfiles_file_count, None, na_len( logfiles_files) )

                    self.simulation_status.add( FolderLogInfo.min_set_output_time,        None,               format_time(min_set_output_time,  default_value="") )
                    self.simulation_status.add( FolderLogInfo.max_INPUTS_time,            None,               format_time(max_INPUTS_time,      default_value="") )
                    self.simulation_status.add( FolderLogInfo.exclusion_status,           isNotExcluded_,     "has exclusion"  if len(str_exclusions)>0  else "no exclusion"                  )
                    self.simulation_status.add( FolderLogInfo.exclusion_info,             isNotExcluded_,     str_exclusions              )
                    self.simulation_status.add( FolderLogInfo.reproducibility,            is_reproducible,    str_reproducibility_status  )
                    self.simulation_status.add( FolderLogInfo.setnames_validity,          has_valid_setNames, "valid setnames" if has_valid_setNames else f"invalid setfiles_count: {len(self.getSetFiles())}" )
                    self.simulation_status.add( FolderLogInfo.reproducibility_violations, is_reproducible,    reproducibility_violations     )
                    
                    
                    cleaner_bytes, cleaner_file_count, _, _, all_cleaners_files = self.get_cleaner_size()   
                    all_cleaners_files = [f for files in all_cleaners_files.values() for f in files] 

                    if not has_valid_setNames:
                        cleanable = "status is undefined"
                    elif len(all_cleaners_files)==0:
                        cleanable = "status is cleaned"
                    else:
                        if clean_parameters.params.file_deletion_method==FileDeletionMethod.Delete and self.simulation_status.can_clean():
                            cleanable = "status is part of clean-up"
                        else:
                            cleanable = "status is not cleaned"

                    if not self.simulation_status.can_clean() :             
                        all_cleaners_files = None

                    self.simulation_status.add( FolderLogInfo.cleaning_status, None,   cleanable )
                    self.simulation_status.add( FolderLogInfo.cleanable_files, None,   str(cleaner_file_count) )
                    self.simulation_status.add( FolderLogInfo.cleanable_bytes, None,   str(cleaner_bytes) )
            """
            if not (folder_type==FolderType.StandardSimulation or folder_type==FolderType.StandardSimulation_partial):
                standard_folder = [e.name.lower() for e in self.get_standard_folders() ]
                standard_folder.sort()
                standard_folders = "|".join( standard_folder )
                self.simulation_status.add( FolderLogInfo.standard_folders,     None,  standard_folders )
            """
        except Exception as e:
            if sys.exc_info() is None:
                error_message = f"\"{self.base_path}\";\"{str(e)}\";\"\";\"\";\"\"\n"
            else:    
                exc_type, exc_value, exc_traceback = sys.exc_info()              
                error_message = f"\"{self.base_path}\";\"{str(exc_type)}\";\"{str(exc_value)}\";\"{str(exc_traceback.tb_frame.f_code.co_name)}\";\"{str(exc_traceback.tb_lineno)}\"\n"
            clean_parameters.params.folder_error_queue.put(error_message)
            print(error_message)

        return self.simulation_status, all_cleaners_files