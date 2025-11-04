import os
import string
import random
from enum import Enum
from typing import NamedTuple
from abc import abstractmethod

class CleanStatus(str,Enum):
    KEEP  = "keep"
    CLEAN = "clean"

class SimulationType(str,Enum):
    VTS = "vts"
    HTC = "htc"  #create a HTCFILES subfolder in the simulation

class SimulationLoadcaseType(str,Enum):
    ONE_SET_FILE = "loads"
    MULTIPLE_SET_FILES = "load_release"

class TimeseriesNames_vs_LoadcaseNames(str,Enum):
    MATCH = "match"                             # match               => all matching timeseries can be cleaned. That is generate files with the same name as the loadcase 
    DIFFERENT = "different"                     # different           => all different timeseries cannot be cleaned. That is generate files with different names than the loadcase. just append "_" to the loadcase name 
    MATCH_AND_DIFFERENT = "match_and_different" # match_and_different => some timeseries can be cleaned, some cannot. That is generate files with:
                                                #                          the same name as the loadcase for half of the loadcases
                                                #                          with different names than the loadcase for the other half 

def generate_random_files(number_of_files:int, folder_path:str, reserved_extension:str):
    # generate 10 file with random names 3-10 and extension with 2-3 charaters except .set files
    # the purpose is just to verify that the application picks the correct files .set files and ignores the rest
    def draw_extension():
        while True:
            ext = ''.join(random.choices(string.ascii_lowercase, k=random.randint(2, 3)))
            if ext != reserved_extension:
                return ext

    generated_filepaths:list[str] = []
    for _ in range(number_of_files):
        random_name       = ''.join(random.choices(string.ascii_lowercase, k=random.randint(3, 10)))
        random_extension  = draw_extension()
        random_file = os.path.join(folder_path, f"{random_name}.{random_extension}")
        with open(random_file, "w") as f:
            f.write("This is a random test file.")
            generated_filepaths.append(random_file)

    return generated_filepaths


class GenerateTimeseries:
    def __init__(self, simulation_generator:"Base_Simulation_Generator", timeseries_count:int, local_folder_names:list[str], extensions:list[str]):
        self.simulation_generator = simulation_generator
        self.timeseries_count     = timeseries_count
        self.local_folder_names   = local_folder_names 
        self.extensions           = (*extensions,)

    def generate_filepaths(self, local_folder: str, loadcase_names: str, ext: str, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames):
        # generate the files with extention "ext" and
        # filename according to the loadcase names AND timeseries_names_vs_loadcase (see class TimeseriesNames_vs_LoadcaseNames)
        match timeseries_names_vs_loadcase:
            case TimeseriesNames_vs_LoadcaseNames.MATCH:
                return [os.path.join(local_folder, f"{loadcase_name}{ext}") for loadcase_name in loadcase_names]
            case TimeseriesNames_vs_LoadcaseNames.DIFFERENT:
                return [os.path.join(local_folder, f"{loadcase_name}_{ext}") for i, loadcase_name in enumerate(loadcase_names)]
            case TimeseriesNames_vs_LoadcaseNames.MATCH_AND_DIFFERENT:
                return [os.path.join(local_folder, f"{loadcase_name}{ext}") if i % 2 == 0 else os.path.join(local_folder, f"{loadcase_name}_{ext}") for i, loadcase_name in enumerate(loadcase_names)]
    
    def generate_files(self, base_path:str, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames ) -> dict[str,os.DirEntry]:
        loadcase_config: LoadcaseConfiguration = self.simulation_generator.getLoadcaseConfiguration()
        setfilepath_with_names: dict[str, list[str]] = loadcase_config.loadcase_path_and_names

        for set_filepath, loadcase_names in setfilepath_with_names.items(): #we can have multiple set files
            # generate the files in the target path
            for local_folder in self.local_folder_names: #we can have multiple subfolder
                # @TODO inthin we can only have one but multiple extentiosn pr subfolder. See : https://dev.azure.com/vestas/DCES/_workitems/edit/907634
                for ext in self.extensions: #we can have multiple extensions pr subfolder 
                    #generate the target path
                    target_path = os.path.join( base_path, local_folder)
                    os.makedirs(target_path, exist_ok=True)
                    filepaths = self.generate_filepaths(local_folder, loadcase_names, ext, timeseries_names_vs_loadcase)
                    #create the files with 1 byte content
                    for filepath in filepaths:  
                        full_filepath = os.path.join( base_path, filepath)
                        with open(full_filepath, "wb") as f:
                            f.write( b"x" )  

        # gather all files in the target folders
        all_entries = dict[str,os.DirEntry]()
        for local_folder in self.local_folder_names:
            target_path = os.path.join( base_path, local_folder)

            with os.scandir(target_path) as entries:
                all_entries.update({e.name.lower():e  for e in entries})

        return all_entries

    @abstractmethod
    def generate(self, base_path:str, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames):
        raise NotImplementedError()


class LoadcaseConfiguration(NamedTuple):
    loadcase_path_and_names: dict[str,list[str]]
    clean_value: CleanStatus
class Base_Simulation_Generator :
    def __init__(self, base_path:str, loadcase_ranges_filepath:str, sim_type: 
                 SimulationType, sim_loadcase_type: SimulationLoadcaseType, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames):
        self.base_path                    = base_path
        self.loadcase_ranges_filepath     = loadcase_ranges_filepath
        self.sim_type                     = sim_type
        self.sim_loadcase_type            = sim_loadcase_type
        self.timeseries_names_vs_loadcase = timeseries_names_vs_loadcase
    
    @abstractmethod
    def getLoadcaseConfiguration(self) -> LoadcaseConfiguration:
        raise NotImplementedError()
