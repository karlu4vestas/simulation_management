from .clean_folder_type import clean_folder_type

# -------------------------------
# For each cleaner. Only include 
# - files with that match the cleaners extensions and files with names from the .set file
# - for clean_all_but_one_pr_ext files we also to not delete the file setname.ext where 
#   the setname is the first setname from the .set that is also found in the phase2 folder


class clean_all_but_one_pr_ext(clean_folder_type):
    def __init__(self, local_folder_names, extensions):
        super().__init__(local_folder_names, extensions)

    def retrieve_file_list(self, simulation, base_path):
        entries_to_delete = {}

        # simulation.get_set_output_files_for_cleaners().get(self.key,[]) account for  len(set_names) so simplification should be possible
        set_names = simulation.getSetNames()
        if len(set_names) > 0:

            try:
                # create dict with filename as key and value with the entry from scandir
                entries = simulation.get_set_output_files_for_cleaners().get(self.key, [])
                entries = {e.name.lower(): e for e in entries}

                # remove first setname file for each extension and send the rest to deletion in the function' return value
                set_entries = set(entries)
                for ext in self.extensions:
                    # its important to iterate over set_names in order to remove the first co-occures in a vts output file and setname  
                    entries_ext = {(sn + ext): entries[sn + ext] for sn in set_names if sn + ext in set_entries}

                    # remove the first
                    if len(entries_ext) > 0:
                        remove = next(iter(entries_ext.keys()))
                        entries_ext.pop(remove)

                    entries_to_delete.update(entries_ext)
            except Exception as e:
                entries_to_delete = {}

        files_to_delete = entries_to_delete.values()
        files_to_delete = [v for k, v in entries_to_delete.items()]
        return files_to_delete
