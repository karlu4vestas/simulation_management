# Example: Dry run
python -m smb_file_processor.main --mode=noop --input="C:\path\to\your\files.csv"

# Example: Deletion with file counting
python -m smb_file_processor.main --mode=delete --input="\\share\data\list.csv" --threads=128 --count_files