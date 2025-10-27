import os
from  pathlib import Path
import shutil
import time
from datetime import datetime
import zipfile
import io
import threading
from queue import Queue, Empty
from multiprocessing import Value

#robust io
class IO:
    size_of_buffer = 2**18 

    @staticmethod
    def write(file_path, data_to_write) :
        failures = 0
        succes   = False
        while failures < 5 and succes is False:
            try:
                with open(file_path, 'wb', buffering=RobustIO.size_of_buffer) as file:
                    bytes_written = file.write(data_to_write)
                    succes = True
            except IOError:
                failures = failures + 1
                print(f"Unable to open or write file for writing:{file_path} attempts {failures}")
                time.sleep(failures)

    @staticmethod
    def read(file_path) :
        failures = 0
        succes   = False
        content  = None
        while failures < 5 and succes is False:
            try:
                with open(file_path, 'rb', buffering=RobustIO.size_of_buffer) as f:
                    content = f.read() 
                succes = True
            except IOError:
                failures = failures + 1
                print(f"Unable to open or read the file: {file_path} after attempts {failures}")
                time.sleep(failures)
        return content    

    @staticmethod
    def exist_path(path) : #USED BY SCANNER
        failures = 0
        succes   = False
        exist    = None
        while failures < 5 and succes is False:
            try:
                exist  = Path(path).exists()
                succes = True
            except IOError:
                failures = failures + 1
                print(f"Unable tjek existance of:{Path} after attempts {failures}")
                time.sleep(failures)
        return exist

    @staticmethod #USED BY SCANNER
    def create_folder(path, exist_ok=True):
        failures = 0
        succes   = False
        while failures < 5 and succes is False:
            try:
                os.makedirs(path, exist_ok=exist_ok)
                succes = True
            except IOError:
                failures = failures + 1
                print(f"failed to create:{path} after attempts {failures}")
                time.sleep(failures)

    @staticmethod
    def delete_folder_tree(path,ignore_errors):
        failures = 0
        succes = False
        while failures < 5 and succes is False:
            try:
                shutil.rmtree(path, ignore_errors=ignore_errors)
                succes = True
            except IOError:
                failures = failures + 1
                print(f"failed delete :{path} after attempts {failures}")
                time.sleep(failures)

    @staticmethod  #USED BY SCANNER
    def delete_file(file_path):
        failures = 0
        succes = False
        while failures < 5 and succes is False:
            try:
                p=Path(file_path)
                if p.exists():
                    Path(file_path).unlink()
                succes = True
            except IOError:
                failures = failures + 1
                print(f"failed to delete file:{file_path} after attempts {failures}")
                time.sleep(failures)

    @staticmethod
    def get_file_list(folder_root) :
        failures = 0
        succes = False
        files = None
        while failures < 5 and succes is False:
            try:
                files = [os.path.join(root, file) for root, dirs, files in os.walk(folder_root) for file in files]
                succes = True
            except IOError:
                failures = failures + 1
                print(f"Unable to identify all files under : {folder_root} after attempts {failures}")
                time.sleep(failures)
        return files
        
    @staticmethod
    def getDirectories(root_folder, ignore_error=True) :
        #print(f"getDirectories root_folder:{root_folder}")
        failures = 0
        max_failure = 1 if ignore_error else 5
        succes = False
        dirs = []
        files = []
        while failures < max_failure and not succes:
            try:
                with os.scandir(root_folder) as ite:
                    for entry in ite:
                        p = os.path.join(root_folder, entry.name)
                        if entry.is_file():
                            files.append( p )
                        else:
                            dirs.append( p )
                succes = True
            except IOError:
                failures = failures + 1
                if not ignore_error:
                    print(f"Unable to identify all files under : {root_folder} after attempts {failures}")
                    time.sleep(1)
        return (root_folder, dirs, files)    
