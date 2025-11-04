import os
import csv
import uuid
import time
import shutil
from concurrent.futures import ThreadPoolExecutor

from GenerateTimeseries import GenerateTimeseries, SimulationType, SimulationLoadcaseType, TimeseriesNames_vs_LoadcaseNames
from GenerateSimulation import GenerateSimulation

# the generation of test data results in the testdata and a validation.csv.
# Use "main_validate_cleanup.py" to expected vs actual existence of files in the cleaned testdata folders

TEST_STORAGE_LOCATION = "/workspaces/simulation_management/VSM/io_dir_for_storage_test"

def generate_simulations(base_path, sim_count, loadcases_ranges_filepath):

    #create simulation path with and without loadrelease parent
    simulation_folders = {  os.path.join(base_path, "loadrelease",  "LOADS"):SimulationType.VTS,
                            #os.path.join(base_path, "sim_with_htc", "LOADS"):SimulationType.HTC  
                         }
    simulations = []
    for p, sim_type in simulation_folders.items():
        simulations.append( GenerateSimulation( 
            base_path=p, 
            loadcase_ranges_filepath=loadcases_ranges_filepath, 
            sim_type=sim_type,
            sim_loadcase_type=SimulationLoadcaseType.ONE_SET_FILE,
            timeseries_names_vs_loadcase=TimeseriesNames_vs_LoadcaseNames.MATCH
        ) )

    validations = dict()
    with ThreadPoolExecutor() as executor:
        for files_status in executor.map(lambda sim: sim.generatefiles(), simulations): 
            validations.update(files_status)


    #save alle the folder to a file that we can use for the clean up
    file_with_simulation_folders = os.path.join(base_path, "simulations.csv")
    with open(file_with_simulation_folders, "w", encoding = "utf-8") as file:
        writer = csv.writer(file,delimiter=";", lineterminator="\n", quoting=csv.QUOTE_ALL)
        for path in simulation_folders:
            writer.writerow([path])

    #save validation folder to a csv-file
    file_clean_up_validation = os.path.join(base_path, "validation.csv")
    with open(file_clean_up_validation, "w", encoding = "utf-8") as file:
        writer = csv.writer(file,delimiter=";", lineterminator="\n", quoting=csv.QUOTE_ALL)
        writer.writerow(["expected_status", "local_folder", "path", "simulation"])
        
        #I should have used the list simulation_folders to make the following more generic
        for path,clean_up_value in validations.items():
            separator = os.path.sep+"LOADS"
            loads_path = path.split(separator)[0]+separator
            local_path = path[len(loads_path)+1:].split(os.path.sep)[0]
            row        = [clean_up_value, local_path, path, loads_path]
            writer.writerow(row)

    return file_with_simulation_folders,file_clean_up_validation, validations


def initialize_generate_simulations (n_simulations:int, base_path:str, loadcases_ranges_filepath:str ):
    
    # delete the old_folder
    if os.path.isdir(base_path ):
        print(f"remove: {base_path}") 
        shutil.rmtree(base_path)
        time.sleep(0)

    print(f"\ncreate : {base_path}") 
    os.makedirs(base_path,exist_ok=True)    

    # create a number of simulations
    start_time = time.perf_counter()
    file_with_simulation_folders, file_clean_up_validation, validations = generate_simulations( base_path, n_simulations, loadcases_ranges_filepath )
    print(f"{time.perf_counter() - start_time:.2f} seconds to create {n_simulations} simulations with files:{len(validations)}")
    print(f"look her for the list of simulations:{file_with_simulation_folders}\nlook here for the clean up validation:{file_clean_up_validation}")


def main():
    #initialize the generators
    timeseries_count = 5
    # Use your existing setup to create directories and manage file paths
    base_path = os.path.join( TEST_STORAGE_LOCATION, "vts_clean_data/test")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    loadcases_ranges_filepath = os.path.join(script_dir, "loadcases", "loadcases_ranges.json")
    initialize_generate_simulations (n_simulations=timeseries_count,base_path=base_path, loadcases_ranges_filepath=loadcases_ranges_filepath)

if __name__ == "__main__":
    main()