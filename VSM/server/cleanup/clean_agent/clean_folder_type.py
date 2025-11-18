from __future__ import annotations
import os  # due to BaseSimulation in get_output_files 

KEEP = "keep"
CLEAN = "clean"


class clean_folder_type:
    def __init__(self, local_folder_names: list[str], extensions: list[str]):
        # the key is the l0 foldername in lower case
        self.key: str = os.path.normpath(local_folder_names[0].lower())
        self.local_folder_names: list[str] = local_folder_names
        self.extensions: tuple[str, ...] = (*extensions,)

    # return the list of files to be deleted
    def retrieve_file_list(self, simulation: BaseSimulation, base_path: str) -> list[os.DirEntry]:
        raise NotImplementedError


class BaseSimulation:
    def __init__(self, base_path: str):
        self.base_path: str = base_path

    def hasValidSetNames(self) -> bool:
        raise NotImplementedError

    def getSetNames(self) -> list[str]:
        raise NotImplementedError

    def get_cleaner_files(self) -> dict[str, list[os.DirEntry]]:
        raise NotImplementedError
    
    def get_set_output_files_for_cleaners(self) -> dict[str, list[os.DirEntry]]:
        raise NotImplementedError
