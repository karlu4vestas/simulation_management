import os
from datetime import datetime
from tests.generate_vts_simulations.GenerateTimeseries import CleanStatus,GenerateTimeseries, LoadcaseConfiguration, TimeseriesNames_vs_LoadcaseNames

#For each extention "ext" this generator 
#   KEEPs the first file that match the first a set name from the .set file starte from the first setname. The rest is set to clean_value 
#   if no setname is found then alle the files with "ext" will be kept
#   The generator keeps all files that do not match an extension
class Generate_clean_all_but_one_pr_ext(GenerateTimeseries):

    #notice that extension can be a tuples array
    def __init__(self, simulation_generator, timeseries_count, local_folder_names, extensions, modified_date:datetime=None ):
        super().__init__(simulation_generator, timeseries_count, local_folder_names, extensions, modified_date)

    def generate(self, base_path:str, timeseries_names_vs_loadcase: TimeseriesNames_vs_LoadcaseNames) -> dict[str, CleanStatus]:
        # generate the test files and evaluate which files to keep and clean

        all_entries:dict[str,os.DirEntry] = self.generate_files(base_path, timeseries_names_vs_loadcase)

        # get all loadcases files and loadcase names
        loadcase_config: LoadcaseConfiguration = self.simulation_generator.getLoadcaseConfiguration()
        all_set_names = { name for names in loadcase_config.loadcase_path_and_names.values() for name in names } #repack all names into one set
        clean_value   = loadcase_config.clean_value

        #notice that entries_to_evaluate if different from the one in Generate_clean_all_pr_ext,
        #  because here we need to match on the loadcase name to keep the first file found for each extension. see below entries_to_evaluate.pop
        entries_to_evaluate:dict[str, os.DirEntry]     = {name:e for name, e in all_entries.items() if name.endswith(self.extensions) and ( name.rsplit(".",1)[0] in set(all_set_names) ) }       
        entries_to_keep:dict[os.DirEntry, CleanStatus] = {e:CleanStatus.KEEP for name, e in all_entries.items() if not name in entries_to_evaluate }


        #find the file to keep for each extension in self.extension and remove it from the list of files to clean.
        # if we do not find a file to KEEP then no files should be deleted
        for ext in self.extensions:

            # so here we do handle multiple loadcase files by removing for each loadcase file the first found file for each extension for each
            for names in loadcase_config.loadcase_path_and_names.values():
                removed_file = None
                
                for sn in names:
                    removed_file = entries_to_evaluate.pop( (sn+ext).lower(), None)
                    if not removed_file is None: 
                        break

                if removed_file is None:
                    print(f"not setname was found for extension:{ext}")            
                    #no set name with extension "ext" was found. Therefore all entries with "ext" must be kept
                    entries_to_keep.update( {e:CleanStatus.KEEP for name,e in entries_to_evaluate.items() if name.endswith(ext) } )
                else:
                    #a set name with extension "ext" was found. KEEP it and set the rest of the entries with "ext" to clean_value
                    entries_to_keep[removed_file] = CleanStatus.KEEP
                    entries_to_keep.update( { e:clean_value for name,e in entries_to_evaluate.items() if name.endswith(ext) } )
        
        #print(entries_to_keep)
        return { e.path:keep_or_clean for e,keep_or_clean in entries_to_keep.items() }