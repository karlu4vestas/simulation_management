import os
from datetime import datetime
from tests.generate_vts_simulations.GenerateTimeseries import CleanStatus,GenerateTimeseries, TimeseriesNames_vs_LoadcaseNames, LoadcaseConfiguration

#The generator set all files with extension to cleanup if the folder is a candidate for clean up. Ie. to clean_value
class Generate_clean_all_pr_ext(GenerateTimeseries):
    def __init__(self, simulation_generator, timeseries_count, local_folder_names, extensions, modified_date:datetime=None ):
        super().__init__(simulation_generator, timeseries_count, local_folder_names, extensions, modified_date)

    def generate(self, base_path:str, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames) -> dict[str, CleanStatus]:
        # generate the test files and evaluate which files to keep and clean

        all_entries:dict[str,os.DirEntry] = self.generate_files(base_path, timeseries_names_vs_loadcase)

        # get all loadcases files and loadcase names
        loadcase_config: LoadcaseConfiguration = self.simulation_generator.getLoadcaseConfiguration()
        all_set_names = { name for names in loadcase_config.loadcase_path_and_names.values() for name in names } #repack all names into one set
        clean_value   = loadcase_config.clean_value


        # ----------------------Evaluate clean status of all files  --------------------------------------------
        # in Generate_clean_all_pr_ext we must set clean status to "clean_value" for all files with the given extensions that are known loadcase files from getSetNames()
        # See the above calculation of "clean_value". In the normal case with one .set file and a normal vts simulation this would mean clean all loadcase files with the given extensions and keep the rest

        # split the files into two groups:
        #  1) use entries_to_evaluate for files that ends with a) one of self.extensions or b) that are not in known loadcases
        clean_value_entries:dict[os.DirEntry, CleanStatus] = {e:clean_value for name, e in all_entries.items() if name.endswith(self.extensions) and ( name.rsplit(".",1)[0] in set(all_set_names) ) }
        
        #  2) put the remaining files in entries_to_keep. They will be kept
        entries_to_keep:dict[os.DirEntry, CleanStatus]     = {e:CleanStatus.KEEP for name, e in all_entries.items() if not e in clean_value_entries }

        # return all the evaluations
        return { e.path:cv for e,cv in {**clean_value_entries, **entries_to_keep}.items() }