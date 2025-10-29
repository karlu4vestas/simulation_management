import argparse
import os
import time
from queue import Queue
from threading import Thread, Event 
from threading import active_count
from multiprocessing import Value
from typing import NamedTuple

import RobustIO
from ProgressWriter import ProgressReporter
from scanner import ScanParameters, ScanPathConfig, Scanner


class ScanResult(NamedTuple):
    """Result of a directory scan operation."""
    nb_scanned_folders: int
    scanned_root_folders: list[str]
    scan_output_files: list[str]
    error_log_files: list[str]


def do_scan(scan_path:str, output_archive:str, nbScanners:int, scan_subdirs:bool, progress_reporter:ProgressReporter) -> ScanResult:
    # Organise the output from the scanning under a folder "output_archive/scanned_folder" for each folder in scan_path 
    # Under "scanned_folder" create the following files
    #  - scan.csv:  for the scan results
    #  - error_log: for logging errors that occur during the scan

    # The ProgressWriter produces one log no matter the value of scan_subdirs. It will be written to scan_subdirs/scan_path/log.csv

    # scan_subdirs=True means that all subfolders of scan_path will be scanned separately. 
    # scan_subdirs=True means that scanning is limited to scan_path such that there will only be one "scanned_folder"

    #returns a tuple with
    #  number of scanned folders
    #  list of scanned root folders
    #  list of scan output files
    #  list of error log files

    # ------ start checking and preparing how the scan of folders must be done and the output organised ----------
    scan_path      = os.path.normpath(scan_path)
    output_archive = os.path.normpath(output_archive)

    if not RobustIO.IO.exist_path(scan_path)  :
        raise FileNotFoundError(f"Paths does not exist: {scan_path}")

    #get the list of folders to scan as root folders
    rootfolders_to_scan: list[str] = []
    if scan_subdirs:
        # adjust the output archive so that reporting of all subfolder is inside a folder with the same from the scan folder
        scan_folder_name:str = os.path.basename(scan_path)
        output_archive:str   = os.path.join( output_archive, scan_folder_name)
        with os.scandir(scan_path) as ite:
            rootfolders_to_scan: list[str] = [entry.path for entry in ite 
                                        if entry.is_dir() and (not "y-migrated\\apps" in entry.path.lower()) and \
                                          (not "y:\\apps" in entry.path.lower()) and (not "\\.snapshot" in entry.path.lower())]
    else:
        rootfolders_to_scan.append(scan_path)

    #ensure that the main output folder exists
    if not RobustIO.IO.exist_path(output_archive) :
        RobustIO.IO.create_folder(output_archive)
        if not RobustIO.IO.exist_path(output_archive) :
            raise FileNotFoundError(f"Failed to create output folder: {output_archive}")

    # ------ DONE checking preparing paths  ----------


    params: ScanParameters = ScanParameters()
    params.nbScanners = nbScanners

    scan_output_files: list[str] = [] 
    errorlog_files: list[str] = []
    for folder in rootfolders_to_scan:
        config: ScanPathConfig = ScanPathConfig(folder, output_archive)
        params.scanpath_config.append( config )
        scan_output_files.append( config.scan_output_file )
        errorlog_files.append( config.scan_output_errorlog_file )

    try:
        job = Thread(target=Scanner.start, args=( params, ), daemon=True )
        job.start()

        #wait for scanner to be done.VSCodeCounter or the scan to exist
        while not params.scan_is_done_event.is_set() and not params.scan_abort_event.is_set():
            progress_reporter.update(params.nb_processed_folders.value, params.io_queue.qsize(), active_count())
            time.sleep(progress_reporter.seconds_between_update)

    except KeyboardInterrupt: #@TODO this will not work for a background service 
        #print("scanning interupted")
        Scanner.stop(params)

    #if the scan is aborted then wait for the output to finish properly
    if params.scan_abort_event.is_set():
        Scanner.stop(params)

    progress_reporter.update(params.nb_processed_folders.value, params.io_queue.qsize(), active_count())

    return ScanResult(
        nb_scanned_folders=params.nb_processed_folders.value,
        scanned_root_folders=rootfolders_to_scan,
        scan_output_files=scan_output_files,
        error_log_files=errorlog_files
    )


# example:  python scan.py "\\?\UNC\vestas\common\Y-migrated\_Temp\karlu" "C:/Users/karlu/Downloads/output/_Temp_reports"
# example 2: py -O scan.py "\\?\Y:\"  ../../data/scan_metadata --nScanners=512  --owner_threads=32
# example 3: py -O scan.py "\\?\UNC\\aumelfile11\vaus"  ../../data/scan_metadata --nScanners=512  --owner_threads=32
#"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan directories and save results.")
    parser.add_argument("scan_path",          type=str,  help="Path of the folder to scan")
    parser.add_argument("output_archive",     type=str,  help="Path of the scan result ")
    parser.add_argument("--nScanners",        type=int,  help="The number of scanning threads. Defaults are 256 with owner_extract else 1024")

    #parser.add_argument("help="min folders for owner extraction threadpools")
    args = parser.parse_args()    
    scan_path = os.path.normpath(args.scan_path)
    output_archive = os.path.normpath( args.output_archive )

    if args.nScanners is None:
        nScanners = 1024
    else: 
        nScanners = args.nScanners

    from ProgressWriter import ProgressReporter, ProgressWriter
    progress_reporter = ProgressWriter( seconds_between_update=5, seconds_between_filelog=60)
    progress_reporter.open(output_archive)

    scan_result = do_scan( scan_path, output_archive, nScanners, scan_subdirs=False, progress_reporter=progress_reporter)

    progress_reporter.close()

#"""

"""    
#default values
nScanners        = 1
#nScanners        = 512
owner_threads    = 32
max_owners       = -1

#scan_path      = os.path.abspath( "\\\\?\\UNC\\vestas\\common\\Y-migrated\\_Temp\\karlu")
scan_path      = os.path.abspath( "\\\\?\\UNC\\vestas.net\\common\\DTS_migration_excel_references")
#scan_path      = os.path.abspath("\\\\?\\Y:\\_Temp\\karlu")
output_archive = os.path.normpath("C:/Users/karlu/Downloads/output/_Temp")
report_archive = os.path.normpath("C:/Users/karlu/Downloads/output/_Temp_reports")

#scan_path      = os.path.abspath( "\\\\?\\UNC\\vestas.net\\common\\Y-migrated\\Group_Technology_RD\\Nacelle - Hub - Tower\\Load - Aerodynamic - Control\\Aerodynamics")
#output_archive = os.path.normpath("C:/Users/karlu/Downloads/output/LAC")
#report_archive = os.path.normpath("C:/Users/karlu/Downloads/output/LAC_reports")

def main():
    do_scan( scan_path, output_archive, nScanners, max_owners, owner_threads)
"""
