import os
import csv
import time
import shutil
from typing import NamedTuple
from concurrent.futures import ThreadPoolExecutor

from tests.generate_vts_simulations.GenerateTimeseries import CleanStatus, SimulationType, SimulationLoadcaseType, TimeseriesNames_vs_LoadcaseNames
from tests.generate_vts_simulations.GenerateSimulation import GenerateSimulation
from tests import test_storage

# the generation of test data results in the testdata and a validation.csv.
# Use "main_validate_cleanup.py" to expected vs actual existence of files in the cleaned testdata folders

TEST_STORAGE_LOCATION = test_storage.LOCATION


class GeneratedSimulationsResult(NamedTuple):
    # Result from generating simulations with validation data
    simulation_paths: list[str]  # List of simulation folder paths for easy iteration
    simulations_csv_file: str  # Path to CSV file listing all simulation folders
    validation_csv_file: str  # Path to CSV file with expected cleanup validation
    validations: dict[str, CleanStatus]  # Dict mapping file paths to expected cleanup status

class SimulationTestSpecification(NamedTuple): 
    fullpath: str
    sim_type: SimulationType

def generate_simulations(base_path:str, simulation_folders: SimulationTestSpecification, loadcases_ranges_filepath:str) -> GeneratedSimulationsResult:

    simulations = []
    for p, sim_type in simulation_folders:
        simulations.append( GenerateSimulation( 
            base_path=p, 
            loadcase_ranges_filepath=loadcases_ranges_filepath, 
            sim_type=sim_type,
            sim_loadcase_type=SimulationLoadcaseType.ONE_SET_FILE,
            timeseries_names_vs_loadcase=TimeseriesNames_vs_LoadcaseNames.MATCH
        ) )

    validations:dict[str, CleanStatus] = {}
    with ThreadPoolExecutor() as executor:
        for files_status in executor.map(lambda sim: sim.generatefiles(), simulations): 
            validations.update(files_status)


    #save alle the folders to a file that we can use for the clean up
    if not os.path.exists(base_path):
        os.makedirs(base_path, exist_ok=True)
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

    return GeneratedSimulationsResult(
        simulation_paths=[ path for path,sim_type in simulation_folders],
        simulations_csv_file=file_with_simulation_folders,
        validation_csv_file=file_clean_up_validation,
        validations=validations
    )


def initialize_generate_simulations (n_simulations:int, base_path:str, loadcases_ranges_filepath:str ) -> GeneratedSimulationsResult:
    
    # delete the old_folder
    if os.path.isdir(base_path ):
        print(f"remove: {base_path}") 
        shutil.rmtree(base_path)
        time.sleep(0)

    print(f"\ncreate : {base_path}") 
    os.makedirs(base_path,exist_ok=True)    

    # create a number of simulations
    start_time = time.perf_counter()
    #create two simulations with different structure
    simulation_folders: SimulationTestSpecification = [
        (os.path.join(base_path, "loadrelease",  "LOADS"), SimulationType.VTS),
        (os.path.join(base_path, "sim_without_htc", "LOADS"), SimulationType.VTS)
    ]

    result:GeneratedSimulationsResult = generate_simulations( base_path, simulation_folders, loadcases_ranges_filepath )
    print(f"{time.perf_counter() - start_time:.2f} seconds to create {n_simulations} simulations with files:{len(result.validations)}")
    print(f"look her for the list of simulations:{result.simulations_csv_file}\nlook here for the clean up validation:{result.validation_csv_file}")
    
    return result

def main():
    #initialize the generators
    # Use your existing setup to create directories and manage file paths
    base_path = os.path.join( TEST_STORAGE_LOCATION, "vts_clean_data/test")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path_to_loadcases_configuration_file = os.path.join(script_dir, "loadcases", "loadcases_ranges.json")


    # delete the old_folder
    if os.path.exists(base_path ):
        shutil.rmtree(base_path)
        time.sleep(0)

    os.makedirs(base_path,exist_ok=True)    
    if not os.path.exists(base_path ):
        raise FileNotFoundError(f"Failed to create directory: {base_path}")
    else:
        print(f"\ncreated : {base_path}")

    # create a number of simulations
    start_time = time.perf_counter()

    simulation_folders: SimulationTestSpecification = [  
        (os.path.join(base_path, "loadrelease",  "LOADS"), SimulationType.VTS),
        #(os.path.join(base_path, "sim_without_htc", "LOADS"), SimulationType.VTS)
    ]

    result:GeneratedSimulationsResult = generate_simulations( base_path, simulation_folders, path_to_loadcases_configuration_file )

    print(f"{time.perf_counter() - start_time:.2f} seconds to create {len(simulation_folders)} simulations with files:{len(result.validations)}")
    print(f"look her for the list of simulations:{result.simulations_csv_file}\nlook here for the clean up validation:{result.validation_csv_file}")

if __name__ == "__main__":
    main()