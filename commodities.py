import dynamic_function as d
database_name='DTCC'
collection_name = 'commodities'
folder_path = '/home/ec2-user/Documents/DTCC_data/extracts/Commodities'
processed_files = set()  # Initialize an empty set to store processed filenames
resulting_dataframes, processed_files = d.read_csvs_with_dates(folder_path, processed_files)
for df in resulting_dataframes:
    d.insert_dataframe_to_mongodb(df,  database_name,collection_name )