import os
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile
from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException, ElementNotInteractableException
import os
import zipfile
import pandas as pd
from pymongo import MongoClient
import os
from datetime import datetime




url = "https://pddata.dtcc.com/gtr/cftc/dashboard.do"
while True:
    try:
        response = requests.get(url)
        if response.status_code == 200:
            break  # Successful response, exit the loop
        else:
            print(f"Received status code: {response.status_code}. Retrying in 300 seconds...")
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}. Retrying in 300 seconds...")
    
    time.sleep(300)  # Wait for 300 seconds before retrying

soup = BeautifulSoup(response.content, "html.parser")






def data_downloader(data_type, download_folder, extract_folder):
    
    
    body = soup.find('body', {'id': 'pageBody'})
    table=body.find_next('table')
    tr1=table.find_next('tr')
    tr1=table.find_next('tr')
    td1=tr1.find_next('td')
    div1=td1.find('div', {'id': 'outerWrapper'})
    iframe1=div1.find('iframe', {'id': 'generalSliceFrame'})
    iframe_src = iframe1['src']
        
    # Configure Chrome options and preferences
    chrome_options = Options()
    chrome_prefs = {
        "download.default_directory": download_folder,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    chrome_options.add_experimental_option("prefs", chrome_prefs)
    # Initialize Chrome driver with the configured options
    driver = webdriver.Chrome(options=chrome_options)
    url = iframe_src
    driver.get(url)
    
    # Wait for the content to load
    time.sleep(2)
    
    downloaded_files = set()
    downloaded_files.update(os.listdir(download_folder))
    while True:
        # Click on the specified link
        link_partial_text = data_type
        link = driver.find_element(By.PARTIAL_LINK_TEXT, link_partial_text)
        link.click()
        
        # # Wait for the content to load
        time.sleep(8)
        
        # Find the data div based on the data type
        if data_type == 'Credits':
            data_div_id='creditSwapsData'
        else:
            data_div_id = f'{data_type.lower()}SwapsData'
        data_swaps_div = driver.find_element(By.ID, data_div_id)
        
        # Find all rows in the table within the data_swaps_div
        rows = data_swaps_div.find_elements(By.CSS_SELECTOR, 'tr.tbl1, tr.tbl2')
        
        if not rows:
            print(f"No more new files available for {data_type}. Exiting loop.")
            break
        
        # Variable to track if new files are downloaded in this iteration
        new_files_downloaded = False
        
        # Loop through the rows and click on the download links
        for row in rows:
            try:
                link = row.find_element(By.CSS_SELECTOR, 'td.s1 a[href$=".zip"]')
                download_link = link.get_attribute("href")
                filename = download_link.split("/")[-1]
                
                if filename in downloaded_files:
                    continue
                
                # Click the download link
                try:
                    WebDriverWait(driver, 2).until(EC.element_to_be_clickable(link)).click()
                        # Wait for the browser to download the file
                    
                    # Wait for the browser to download the file
                    
                    
                    print(f"Downloaded: {filename}")
                    downloaded_files.add(filename)
                    new_files_downloaded = True
                    
                    # Extract the downloaded ZIP file to the specified folder
                    zip_path = os.path.join(download_folder, filename)
                    while not os.path.exists(zip_path):
                        time.sleep(3)
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_folder)
                    print(f"Extracted: {filename} to {extract_folder}")
                except ElementNotInteractableException:
                    print(f"Skipped: {filename} (Element not interactable)")
            except (StaleElementReferenceException, NoSuchElementException):
                continue
        
        # If no new files were downloaded in this iteration, exit the loop
        if not new_files_downloaded:
            print(f"No more new files available for {data_type}. Exiting loop.")
            break
        
        # Refresh the page to get the updated list of rows
        driver.refresh()
        time.sleep(4)  # Adjust the wait time based on the page's loading speed
        downloaded_files.update(os.listdir(download_folder))
    
    # Close the browser
    driver.quit()