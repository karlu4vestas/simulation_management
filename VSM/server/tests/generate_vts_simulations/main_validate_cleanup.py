from enum import Enum
import os
import time
import polars as pl 
#from xlsxwriter import Workbook

class CleanStatus(str,Enum):
    KEEP  = "keep"
    CLEAN = "clean"

def validate_cleanup(validation_file: str, simulation_scope:list[str]=None) -> tuple[str, list[str]]:
    #Args: validation_file: path to the validation csv file
    #      simulations_scope: list of simulation names that the validation must focus on
    validation_file = os.path.normpath(validation_file)
    
    if os.path.isfile(validation_file):
        base_path                      = os.path.split(validation_file)[0]
        str_now                        = time.strftime("%Y-%m-%d %H-%M-%S-", time.gmtime())        
        filepath_to_validation_results = os.path.join(base_path, str_now + "test_results.csv")


        #print(f"reading validation file:{validation_file}\n")
        schema_overrides = {"path":pl.String,"expected_status":pl.String}
        validation = pl.read_csv(validation_file, schema_overrides=schema_overrides, encoding = "utf-8", separator=';', quote_char='"' )
        if simulation_scope: # lets focus on the scope
            validation = validation.filter( pl.col("simulation").str.contains_any(simulation_scope, ascii_case_insensitive=True) )

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
        validation.write_csv(filepath_to_validation_results, separator=';')                

        #show the number of issues
        failed_filepaths = validation.filter( pl.col("test_status")=="failed" )["path"].to_list()

        return filepath_to_validation_results, failed_filepaths
    else:
        raise FileNotFoundError(f"did not find:{validation_file}")

def main():
    from tests import test_storage
    TEST_STORAGE_LOCATION = test_storage.LOCATION
    validation_file = os.path.join( TEST_STORAGE_LOCATION, "vts_clean_data/test/validation.csv")
    filepath_to_validation_results, failed_filepaths = validate_cleanup(validation_file)
    print(f"The comparison with the validation file be found here:{filepath_to_validation_results}\n")

    if failed_filepaths:
        print(f"Found {len(failed_filepaths)} failed paths:")
        for path in failed_filepaths:
            print(f" - {path}")
    else:
        print(f"did not find:{validation_file}")

if __name__ == "__main__":
    main()    