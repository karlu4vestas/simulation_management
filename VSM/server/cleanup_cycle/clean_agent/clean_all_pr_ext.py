import os
from .clean_folder_type import BaseSimulation, clean_folder_type

# -------------------------------
# For each cleaner. Only include 
# - files with that match the cleaners extensions and files with names from the .set file

class clean_all_pr_ext(clean_folder_type):
    def __init__(self, local_folder_names, extensions):
        super().__init__(local_folder_names, extensions)

    def retrieve_file_list(self, simulation:BaseSimulation, base_path: str) -> list[os.DirEntry]:
        # simulation.get_set_output_files_for_cleaners().get(self.key,[]) account for  len(set_names) so simplification should be possible
        entries = []
        set_names:list[str] = simulation.getSetNames()
        if len(set_names) > 0:
            entries = simulation.get_set_output_files_for_cleaners().get(self.key, [])

        return entries
