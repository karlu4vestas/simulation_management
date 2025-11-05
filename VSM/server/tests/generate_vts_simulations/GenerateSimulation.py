import os
import re
import io
import string
import random
from concurrent.futures import ThreadPoolExecutor

from tests.generate_vts_simulations.GenerateTimeseries import CleanStatus, Base_Simulation_Generator, GenerateTimeseries
from tests.generate_vts_simulations.GenerateTimeseries import SimulationLoadcaseType, SimulationType
from tests.generate_vts_simulations.GenerateTimeseries import TimeseriesNames_vs_LoadcaseNames, LoadcaseConfiguration, generate_random_files
from tests.generate_vts_simulations.loadcases.loadcases_lc_synth_from_ranges import write_synthetic_setfile
from tests.generate_vts_simulations.GenerateTimeseries_all_pr_ext import Generate_clean_all_pr_ext
from tests.generate_vts_simulations.GenerateTimeseries_all_but_one_pr_ext import Generate_clean_all_but_one_pr_ext

# Generate files an entire set of vts simulation files and the expected results after cleanup. see "def generatefiles(self)"

class GenerateSimulation (Base_Simulation_Generator) :
    vts_standard_folders = ["DETWIND","EIG","INPUTS","INT","LOG","OUT","PARTS","PROG","STA"]
    number_ofLoadcase:int = 6

    #input_selector must be 0 or 1
    def __init__(self, base_path:str, loadcase_ranges_filepath:str, 
                 sim_type: SimulationType, sim_loadcase_type: SimulationLoadcaseType, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames):
        super().__init__(base_path, loadcase_ranges_filepath, sim_type, sim_loadcase_type, timeseries_names_vs_loadcase)
        self.keep_files_defined_by_parent = None
        self.timeseries_generators:list[GenerateTimeseries] = [ 
                                        Generate_clean_all_pr_ext(self, 0,         ["INT"],            [".int", ".tff"] ), 
                                        Generate_clean_all_pr_ext(self, 0,         ["EXTFND"],         [".sim"]         ), 
                                        Generate_clean_all_pr_ext(self, 0,         ["EXTFND/dat"],     [".sim"]         ), 
                                        Generate_clean_all_but_one_pr_ext(self, 0, ["OUT"],            [".out"]         ), 
                                        Generate_clean_all_but_one_pr_ext(self, 0, ["STA"],            [".sta"]         ), 
                                        Generate_clean_all_but_one_pr_ext(self, 0, ["EIG"],            [".eig", ".mtx"] ), 
                                        Generate_clean_all_but_one_pr_ext(self, 0, ["LOG"],            [".log"]         ) 
                                       ]

    #create standard folders and return due to htc files or keywords (loadrelease etc) in the parent path
    def create_base_folder(self, path):
       #create standard folders
        for f in  self.vts_standard_folders:
            os.makedirs( os.path.join(path,f), exist_ok=True )

        # the word htc in the parent path must result in a HTCFILES subfolder 
        has_htcfiles = self.sim_type == SimulationType.HTC
        if has_htcfiles:
            sim_with_htc_subfolder = os.path.join(self.base_path,"HTCFILES")
            os.makedirs(sim_with_htc_subfolder, exist_ok=True)

        return has_htcfiles # or re_folder_exclusions.search(self.base_path) != None


    def generate_INPUTS(self) -> tuple[dict[str,CleanStatus], dict[str,list[str]]]:        
        input_path:str            = os.path.join(self.base_path,"INPUTS")
        input_files:dict[str,CleanStatus] = {path:CleanStatus.KEEP for path in generate_random_files(10, input_path, ["set"])}

        setfiles_names:dict[str,list[str]] = {}
        #generate one or two .set files acccording to the simulation_loadcase_type
        if self.sim_loadcase_type == SimulationLoadcaseType.ONE_SET_FILE:
            set_filepath_1:str = os.path.join(input_path,"loadcase_set_1.set")

            #call the generator to create the set file and get its set names
            out, names = write_synthetic_setfile(set_filepath_1, self.number_ofLoadcase, 2025, self.loadcase_ranges_filepath)
            setfiles_names[set_filepath_1] = names

            input_files[set_filepath_1] = CleanStatus.KEEP
        else:
            set_filepath_1:str = os.path.join(input_path,"loadcase_set_1.set")
            loadcase_path_2:str = os.path.join(input_path,"loadcase_set_2.set")
            #call the generator to create the set files and get their set names
            out, names = write_synthetic_setfile(set_filepath_1, self.number_ofLoadcase, 2025, self.loadcase_ranges_filepath)
            setfiles_names[set_filepath_1] = names
            out, names = write_synthetic_setfile(loadcase_path_2, self.number_ofLoadcase, 3030, self.loadcase_ranges_filepath)
            setfiles_names[loadcase_path_2] = names

            input_files[set_filepath_1]  = CleanStatus.KEEP
            input_files[loadcase_path_2] = CleanStatus.KEEP


        return input_files, setfiles_names

    def getLoadcaseConfiguration(self) -> LoadcaseConfiguration:
        #all_set_names:list[str] = []
        #for names in self.setfiles_names.values():
        #   all_set_names.extend(names)

        # @TODO or if we are not able to determine which loadcase files to keep and which to clean, because of multiple set files 
        # We need to keep all files in two cases: A) if instructed to by parent (keep_files_defined_by_parent == True), B) if there are multiple set files
        # @TODO wonder if we could just clean loadcases from multiple set files as well? : https://dev.azure.com/vestas/DCES/_workitems/edit/907872

        clean_value = CleanStatus.KEEP if ( len(self.setfiles_names) != 1 or self.keep_files_defined_by_parent ) else CleanStatus.CLEAN
        return LoadcaseConfiguration(self.setfiles_names, clean_value)

    def generatefiles(self) -> dict[str, CleanStatus]:
        #in this function we generate all files required for the simulation and report what there clean up status must be when the clean is performed

        #setup the standard vts folders and possible a htc subfolder
        self.keep_files_defined_by_parent = self.create_base_folder(self.base_path)

        # generate random files in the INPUTS folder AND one or more ".set" loadcase files
        # notice that the loadcase are generated according to the loadcase_ranges_filepath provided in the constructor
        inputfile_expectations, setfiles_names = self.generate_INPUTS()
        self.setfiles_names = setfiles_names
        file_expectations: dict[str, CleanStatus] = inputfile_expectations.copy()

        # generate the timeseries files in the standard folders according to the loadcases and the rules in timeseries_generators
        with ThreadPoolExecutor() as executor:
            for d in executor.map(lambda gen: gen.generate(self.base_path, self.timeseries_names_vs_loadcase), self.timeseries_generators) :
                file_expectations.update(d)        

        # return the expected clean status for all generated files 
        return file_expectations