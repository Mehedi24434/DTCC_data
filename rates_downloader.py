import dynamic_function as d
import time
data_type='Rates' 
download_folder= './dynamic_download'
extract_folder= './test_extract/Rates'
while True:
    try:
        d.data_downloader(data_type, download_folder, extract_folder)
    except Exception as e:
        print(f"An exception occurred: {e}")
        print("Retrying in 3 seconds...")
        time.sleep(3)