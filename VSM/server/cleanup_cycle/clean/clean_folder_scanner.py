import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from clean_simulation import Simulation, FolderType 
from clean_writer_tasks import FolderLogInfo
from cleanup_cycle.clean.cleaner import clean_parameters_start_stop
#import RobustIO 

def clean_simulation(sim:Simulation, folder_error_queue, params:clean_parameters_start_stop):
    try:
        folder_type = sim.getFolderType()
        simulation_status, all_files_to_clean = sim.eval(params.file_deletion_method)
        _, sim_count_files, _, _, _  = sim.get_simulation_size()      

        if folder_type==FolderType.StandardSimulation or folder_type==FolderType.StandardSimulation_partial:

            if simulation_status.can_clean() :
                if len(all_files_to_clean) > 0:
                            
                    #files from the file deletion queue are only deleted if params.file_deletion_method==FileDeletionMethod.Delete 
                    # else the files are just logged as if they were deleted
                    if folder_type==FolderType.StandardSimulation or folder_type==FolderType.StandardSimulation_partial:
                        dummy = [ params.file_deletion_queue.put(f) for f in all_files_to_clean ]

                    params.sim_cleaned.increment()
                else:
                    params.sim_already_cleaned.increment()                 
            else:
                #reproducibility = simulation_status.can_clean_criteria.get(str(FolderLogInfo.reproducibility),None)
                #if not reproducibility is None and not reproducibility: 
                #    params.sim_irreproducible.increment()
                #else:
                params.sim_ignored.increment()
                
            params.sim_processed.increment()
        #elif folder_type==FolderType.PartialSimulation:
        #    simulation_status.add( FolderLogInfo.root_folder,     None, sim.base_path )
            
        #    standard_folder = [e.name.lower() for e in sim.get_standard_folders() ]
        #    standard_folder.sort()
        #    standard_folders = "|".join( standard_folder )
        #    params.partial_simulation_parent[sim.base_path] = (sim.base_path, standard_folders)
            simulation_status.add( FolderLogInfo.root_folder, None, sim.base_path )
            standard_folder  = [e.name.lower() for e in sim.get_standard_folders()]
            standard_folder.sort()
            simulation_status.add( FolderLogInfo.standard_folders, None, "|".join( standard_folder ) )

            direct_child_folders = [e.name.lower() for e in sim.get_direct_child_folders()]
            direct_child_folders.sort()
            simulation_status.add( FolderLogInfo.direct_child_folders, None, "|".join( direct_child_folders ) )

        else:
            # retrive the folder root and type and update the list of knwon folder roots 
            #all folder are Normal folder pr default.
            #if a child has a Normal Folder as parent but is not itself af NormalFolder then the childs folder, folder and standars folders type takes preceedence
            head, _ = os.path.split(sim.base_path)
            folder_root, root_standard_folders, folder_root_type = params.folder_root_dict.get(head,(None,None,None))

            standard_folder = [e.name.lower() for e in sim.get_standard_folders() ]
            standard_folder_count = len(standard_folder)
            standard_folder.sort()
            standard_folders = "|".join( standard_folder )


            if folder_root_type is None:
                folder_root = sim.base_path
                folder_root_type = folder_type
                root_standard_folders = standard_folders
            elif folder_root_type == FolderType.PartialSimulation:
                pass
            else:
                folder_root = sim.base_path
                folder_root_type = folder_type
                root_standard_folders = standard_folders

            simulation_status.add( FolderLogInfo.folder_type,      None, str( folder_root_type ) )
            simulation_status.add( FolderLogInfo.root_folder,      None, folder_root )
            simulation_status.add( FolderLogInfo.standard_folders, None, root_standard_folders )

            params.folder_root_dict[sim.base_path] = (folder_root, root_standard_folders, folder_root_type)         

        if sim_count_files > 0 or (folder_type==FolderType.StandardSimulation or folder_type==FolderType.StandardSimulation_partial):
            params.folders_processed.increment()
            params.scan_log_queue.put(simulation_status)

    except Exception as e: 
        params.sim_ignored.increment()
        params.folders_processed.increment()
        if sys.exc_info() is None:
            error_message = f"\"{str(e)}\";\"{sim.base_path}\";\"\";\"\";\"\"\n"
        else:    
            exc_type, exc_value, exc_traceback = sys.exc_info()              
            error_message = f"\"{sim.base_path}\";\"{str(exc_type)}\";\"{str(exc_value)}\";\"{str(exc_traceback.tb_frame.f_code.co_name)}\";\"{str(exc_traceback.tb_lineno)}\"\n"
            folder_error_queue.put(error_message)
        print(error_message)

def folder_scanner( folder_queue, folder_error_queue, params, max_failure=2):
    simulation_threadpool = ThreadPoolExecutor( max_workers = 10  )

    while not params.stop_event.is_set():                
        failures, succes = 0, False
        folder = folder_queue.get()
        #print(f"folder_scanner - folder: {folder}")
        if folder is None:
            folder_queue.task_done()
            break

        while failures < max_failure and not succes:
            try: 
                simulation  = Simulation(folder,simulation_threadpool)
                folder_type = simulation.getFolderType()
                if folder_type == FolderType.MissingFolder :
                    error_message = f"\"missing folder\";\"{folder}\";\"\";\"\";\"\"\n"
                    folder_error_queue.put(error_message)
                else:
                    clean_simulation(simulation:Simulation, folder_error_queue, params)

                #print(f"foldertype:{folder_type} folder:{folder}")
                if not params.stop_event.is_set() and (folder_type==FolderType.NormalFolder or folder_type==FolderType.PartialSimulation) :
                    #schedule the subfolders for scanning
                    dirs = [ entry for entry in simulation.base_entries if entry.is_dir(follow_symlinks=False) ]
                    if len(dirs) > 0:
                        [ folder_queue.put(d.path) for d in dirs ]
                        params.folder_count_total.increment(len(dirs))


                succes = True
            except Exception as e: 
                failures = failures + 1
                print(f"failure: {str(e)}")
                if failures < max_failure:
                    time.sleep(1)
                else:
                    if sys.exc_info() is None:
                        error_message = f"\"{str(e)}\";\"{folder}\";\"\";\"\";\"\"\n"
                    else:    
                        exc_type, exc_value, exc_traceback = sys.exc_info()              
                        error_message = f"\"{folder}\";\"{str(exc_type)}\";\"{str(exc_value)}\";\"{str(exc_traceback.tb_frame.f_code.co_name)}\";\"{str(exc_traceback.tb_lineno)}\"\n"
                        folder_error_queue.put(error_message)
                    print(error_message)
        folder_queue.task_done()
