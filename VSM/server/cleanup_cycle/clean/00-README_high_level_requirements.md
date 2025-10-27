Prompt to improve the high-level specification: Let's make a requirements specification together that you can use to make a Python application. I will give you some high-level requirements. You must ask or propose whatever information you judge missing in order to make a formal requirements specification. Here are my requirements:

**Windows Command-Line (CLI) Application to Remove Files Permanently from a Multi-User SMB Network Storage Platform**

0) The purpose of the application is to delete millions of files read from an input file that may contain more files than the application can hold in the machine's RAM. The application MUST therefore be robust enough to run for days while being in control of memory consumption and efficient enough to delete millions of files in a reasonable time.

1) The application must have 3 run modes:
   - Two dry-run modes. For dry-run, prepend the dry-run mode to the name of the log files. The two dry-run modes are:
     - **mode=noop**: A non-operation mode that exercises all the code without touching the files.
     - **mode=exist**: A mode to verify the existence of each file with up to 3 retries, with half a second between retries.
     - **mode=delete**: To delete files with up to 3 retries per file, with half a second between retries.

2) The files to be removed are read from a UTF-8 encoded CSV file containing one column only, with the full path to the files to remove. All cells are escaped using the sign `"`.

3) The result is written to two files encoded in UTF-8. The columns must be escaped using the character `"` and the separator character `;`.
   - A log file `removed_files-datetime.csv` with a column `files` containing the full path of files that were removed successfully.
   - A log file `failed_files-datetime.csv` with three columns: 
     1. A column `files` containing the full path of files that failed to be removed.
     2. A column `error` with the error message.
     3. A column with datetime `yyyy-mm-dd_hh-mm` for the failure.
   - The datetime used in the file names is in the format `2024-12-24_23-59` (yyyy-mm-dd_hh-mm).

4) The application is run from the command line (CLI).

5) Progress is updated to the command line: seconds since start, number of files deleted, number of failed deletions.

6) The input and output directories can be the same.

7) It must be possible to interrupt the application gracefully.

8) The target language is Python 3.13 or higher.

Once the above high-level requirements specification is sufficient, I used the following prompt to make a formal requirements specification: Here are the updated requirements. Please compile a formal requirements specification in a format that you can use to make an application design before generating the code: