import function as f
database_name='DTCC'
collection_name = 'equities'
folder_path = '/home/mehedi/Documents/docker_influxdb/DTCC_data/extract_files/Equities'
processed_files = set()  # Initialize an empty set to store processed filenames
resulting_dataframes, processed_files = f.read_csvs_with_dates(folder_path, processed_files)
for df in resulting_dataframes:
    f.insert_dataframe_to_mongodb(df,  database_name,collection_name )