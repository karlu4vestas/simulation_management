"""
Clean up VTS simulations. 
-The simulations are specifice in a csv file provided to the application on the command line.
"""
import os
import datetime, time
import argparse
from clean_parameters import FileDeletionMethod
from cleaner import clean_simulations, clean_parameters_start_stop, ft_al_9, ft_al_15, ft_al_6

# read all simulation from the file          
def read_simulation_folders_from_file(file_path)->list[str]:   
    simulation_folders:list[str] = []
    try:
        with open(file_path, "r", encoding = "utf-8-sig") as file:
            for p in file:
                p = p.rstrip('\r\n')
                if p[0]=='"' and p[-1]=='"':
                    p = p.strip('"')
                simulation_folders.append( os.path.normpath(p) )
    except Exception as e:
        print(f"failed to process the file with simulations paths:\n{str(e)}")
        simulation_folders=[]

    return simulation_folders


#test examples:  python .\clean_simulations_from_file.py "C:\Users\karlu\Downloads\vts_clean_data\test\simulations.csv" --method=analyse
#test examples:  python .\clean_simulations_from_file.py "C:\Users\karlu\Downloads\vts_clean_data\test\simulations.csv" --method=delete
#test examples:  python .\clean_simulations_from_file.py "\\vestas.net\common\Y-migrated\_Temp\karlu\test\simulations.csv" --method=analyse
#test examples:  python .\clean_simulations_from_file.py "C:\Users\karlu\Downloads\vts_data\can-clean-ymigrated-2016-2022.csv" --method=analyse
def main( file_of_simulations:str, file_deletion_method:FileDeletionMethod = FileDeletionMethod.Analyse, min_date=None, max_date=None):
    start_time = time.time()

    if os.path.isfile(file_of_simulations):

        params:clean_parameters_start_stop = clean_parameters_start_stop(os.path.dirname(file_of_simulations),file_deletion_method)

        if not min_date is None and not max_date is None:
            params.set_min_max_date(min_date, max_date)
        
        simulation_folders = read_simulation_folders_from_file(file_of_simulations)
        print(f"\nNumber of folders to be processed:{len(simulation_folders)}")
        for p in simulation_folders:
            print(f"{p}")

        print(f"min date:{params.min_date} max_date:{params.max_date}")

        if len(simulation_folders) > 0 :
            processed_simulation, count_cleaned, count_ignored, count_already_cleaned, count_not_reproducible = clean_simulations(simulation_folders, params)
            print( f"\r{params.logStatus()}" )

        params.close_logStatus()
    else:
        print( f"The file with base simulations does NOT exist:{file_of_simulations}" )
    
    print(f"scan in { int(time.time() - start_time +0.5) } seconds")
        


#main("C:/Users/karlu/OneDrive - Vestas Wind Systems A S/Documents/2024-12-19-scan_vts_share/data/vts_share.csv", FileDeletionMethod.Analyse)
#main("//vestas.net/Common/DTS_migration_excel_references/hpc_migrated_apps.csv", FileDeletionMethod.Analyse)
#main("C:/Users/karlu/Downloads/vts_clean_data/test/simulations.csv", FileDeletionMethod.Analyse)
#main("//vestas.net/common/Y-migrated/_Temp/karlu/simulations.csv", FileDeletionMethod.Analyse)
#main("C:/Users/karlu/Downloads/vts_clean_data/test/simulations.csv", FileDeletionMethod.Analyse)
#main("//vestas.net/common/Y-migrated/_Temp/karlu/test/simulations.csv", FileDeletionMethod.Analyse)
"""
def cp(): 
    #main( "//vestas.net/common/Y-migrated/_Temp/karlu/test/simulations.csv", FileDeletionMethod.Analyse, 
    main( "C:/Users/karlu/Downloads/t/antares_jvtask_concept_dev.txt", FileDeletionMethod.Analyse, 
          min_date=datetime.datetime(2016, 1, 1, 00, 00), max_date=datetime.datetime(2024, 4, 1, 00, 00))
cp()
#import cProfile
#cProfile.run("cp()")
#import timeit
#repetitions=20
#print(f"cp {timeit.timeit(lambda: cp(), number=repetitions)}")
"""
#"""
if __name__ == "__main__":
    print(f"Starting the simulation cleaner")
    #Set up the signal handler for Ctrl+C    
    parser = argparse.ArgumentParser(description="Cleanup simulations from a file of base simulation folders")
    parser.add_argument("simulations", type=str, help="file with the list of simulations to be cleaned ")
    parser.add_argument("--method", type=str, help="values:--method=analyse or --method=delete. analyse is default and will only count the freed bytes and files. delete wil also delete the files")
    parser.add_argument("--min_date", type=str, help="ignore all simulaition before minimum date (YYYY-MM-DD)")
    parser.add_argument("--max_date", type=str, help="ignore all simulation after minimum date (YYYY-MM-DD)")
    args   = parser.parse_args()   
    #print(f"args:{args}")
    #print(f"args:{args.simulations}")

    file_of_simulations = os.path.normpath(args.simulations)

    if args.method is None or args.method=="analyse":
        method = FileDeletionMethod.Analyse
    elif args.method=="delete":
        method = FileDeletionMethod.Delete

        answer = input("confirm the the cleanup should run by entering y or yes ?")
        if not answer.lower() in ["y","yes"]:
            method = None
    else:
        method = None

    min_date = max_date = None
    if not args.min_date is None and not args.max_date is None :
        try:
            min_date =  datetime.datetime.strptime(args.min_date, "%Y-%m-%d")
            max_date =  datetime.datetime.strptime(args.max_date, "%Y-%m-%d")
            answer   = input("is this the min and max date to use for the clean up. Confirm by entering y or yes ?")
            if not answer.lower() in ["y","yes"]:
                min_date = max_date = None
                exit()

        except Exception as e:
            print(f"failed to set the min and max date: {str(e)}")
            exit()
    #else we just use the default min and max date set in the clean_parameters.py file        


    if not method is None:
        main(file_of_simulations, method, min_date, max_date)
    else:
        print( f"the method argument is incorrect. Must be one of: absent, --method=analyse or --method=delete" )
#"""