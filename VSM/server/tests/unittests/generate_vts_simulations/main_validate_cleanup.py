from enum import Enum
import os
import time
import polars as pl 
#from xlsxwriter import Workbook

class CleanStatus(str,Enum):
    KEEP  = "keep"
    CLEAN = "clean"

TEST_STORAGE_LOCATION = "/workspaces/simulation_management/VSM/io_dir_for_storage_test"

def validate_cleanup(validation_file):
    #if __name__ == "__main__":
    #Set up the signal handler for Ctrl+C    
    #parser = argparse.ArgumentParser(description="Validate cleanup simulations of testdata")
    #parser.add_argument("--validation_file", type=str, help="The list of file and their expected status for cleanup")
    #args   = parser.parse_args()   
    #validation_file = os.path.normpath(args.validation_file)
    validation_file = os.path.normpath(validation_file)
    

    if os.path.isfile(validation_file):
        base_path               = os.path.split(validation_file)[0]
        str_now                 = time.strftime("%Y-%m-%d %H-%M-%S-", time.gmtime())        
        resultpath_ex_extension = os.path.join(base_path, str_now + "test_results")


        print(f"reading validation file:{validation_file}\n")
        schema_overrides = {"path":pl.String,"expected_status":pl.String}
        validation = pl.read_csv(validation_file, schema_overrides=schema_overrides, encoding = "utf-8", separator=';', quote_char='"' )

        #run through all files and register the existence status to the column "measured_status"
        path_existance = [CleanStatus.KEEP if os.path.exists(p) else CleanStatus.CLEAN for p in validation["path"].to_list()]
        validation = validation.with_columns(pl.Series(name="path_existance", values=path_existance)) 

        #compare the columns:
        # if no difference exist between the two columns the write "The test PASSED: the expected and the measured cleanup of files are identical"
        # if differences exist the write "The test failed. Here are the differences between the expected and the measured clean up of testdata:"
        # and show the list of files where expected_status is different from the measured_status.
        validation = validation.with_columns( 
                        pl.when( pl.col("expected_status") == pl.col("path_existance") )
                                 .then(pl.lit("passed"))
                                 .otherwise(pl.lit("failed"))
                                 .alias("test_status")
                     )               

        validation = validation.select([ "expected_status", "test_status", "path_existance", "local_folder", "path", "simulation"])

        # with open(test_results_path+".xlsx", "wb") as file:
        #     with Workbook(file,) as writer:
        #         validation.write_excel(workbook=writer,worksheet="test_results") 
        validation.write_csv(resultpath_ex_extension+".csv", separator=';')                
        print(f"The comparison with the validation file be found here:{resultpath_ex_extension}\n")

        #show the number of issues
        failed_paths = validation.filter( pl.col("test_status")=="failed" )                              
        print( f"The comparison of expected vs measure file deletions shows the following number of issues:{len(failed_paths)}:")
        if len(failed_paths)>0: 
            print(failed_paths.head())
    else:
        print(f"did not find:{validation_file}")


def main():
    validation_file = os.path.join( TEST_STORAGE_LOCATION, "vts_clean_data/test/validation.csv")
    validate_cleanup(validation_file)

if __name__ == "__main__":
    main()    