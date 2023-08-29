import dynamic_function as d
import time
database_name='DTCC'
collection_name = 'equities'
folder_path = '/home/ec2-user/Documents/DTCC_data/extracts/Equities'
processed_files = set()  # Initialize an empty set to store processed filenames
while True:
    try:
        
        resulting_dataframes, processed_files = d.read_csvs_with_dates(folder_path, processed_files)
        for df in resulting_dataframes:
            d.insert_dataframe_to_mongodb(df,  database_name,collection_name )
            
    except Exception as e:
        print(f"An exception occurred: {e}")
        print("Retrying in 3 seconds...")
        time.sleep(3)