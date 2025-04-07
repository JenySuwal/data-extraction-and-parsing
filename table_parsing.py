from celery import Celery
import json
import time
import redis
import pandas as pd
import boto3
import numpy as np
from clean_html import clean_html_task
from bs4 import BeautifulSoup
import os
from parsing_on_batch import process_html_and_extract_data
from extract_table import create_dataframes

s3_client = boto3.client("s3")
INPUT_BUCKET = "scraped-unstructured-data"
OUTPUT_BUCKET = "parsed-structured-data"

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
celery_app = Celery('tasks', broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')
celery_app.conf.task_routes = {
    "tasks.scrape_tables_task": {"queue": "scraping_queue"},
    "tasks.scrape_pdfs_task": {"queue": "scraping_queue"},
    "tasks.scrape_images_task": {"queue": "scraping_queue"},
    "tasks.scrape_html_task": {"queue": "scraping_queue"},
    "tasks.process_batch": {"queue": "scraping_queue"},
    "tasks.parse_task": {"queue": "parsing_queue"},
}

class FinalDataframe:
    def __init__(self, bucket_name, file_key):
        try:
            self.header_dfs, self.body_dfs = create_dataframes(bucket_name, file_key)  
            if not self.header_dfs or not self.body_dfs:
                raise ValueError("Error in creating dataframes: headers or bodies are empty.")
            
            self.llm_output_list = process_html_and_extract_data(bucket_name, file_key)  
            if not self.llm_output_list:
                raise ValueError("Error in LLM output: No data extracted.")
            
            self.total_tables = len(self.header_dfs)
            self.processed_dfs = []
        
        except Exception as e:
            print(f"Initialization error: {e}")
            self.header_dfs, self.body_dfs, self.llm_output_list, self.total_tables, self.processed_dfs = [], [], [], 0, []

    def process_all_tables(self):

        for i in range(self.total_tables):
            header_df = self.header_dfs[i]
            body_df = self.body_dfs[i]
            llm_output = self.llm_output_list[i]
            merged_df = self.add_header(llm_output, header_df, body_df)
            
            
          
            if merged_df is not None:
                data_dict = json.loads(llm_output)
                merged_df = self.assign_material_surface(data_dict, merged_df)
                merged_df = self.assign_thread_size(data_dict, merged_df)

                final_df = self.merge_header(merged_df)
                
                self.processed_dfs.append(final_df)
            
    @staticmethod
    def add_header(llm_output, header_df, body_df):
        try:
            parsed_dict = json.loads(llm_output)
            keys = list(parsed_dict.keys())

            header_df = header_df.copy()
            current_cols = header_df.shape[1]

            for idx, key in enumerate(keys):
                new_col_name = str(current_cols + idx + 1)
                header_df[new_col_name] = np.nan
                header_df[new_col_name] = header_df[new_col_name].astype('object')
                header_df.iloc[-1, header_df.columns.get_loc(new_col_name)] = str(key)

            combined = pd.concat([header_df.iloc[[-1]], body_df], ignore_index=True)
            combined.columns = combined.iloc[0]
            combined = combined.drop(0).reset_index(drop=True)
            combined.columns = combined.columns.str.strip()
            return combined
        except Exception as e:
            print(f"Error adding header: {e}")
            return None

    @staticmethod
    def assign_thread_size(data_dict, dataframe):
        thread_sizes = {size.replace('"', '').replace('\\', '') for size in data_dict.get('thread_size', [])}  
        if not thread_sizes:
            return dataframe

        df = dataframe.copy()
        first_col = df.columns[0]
        df['thread_size'] = None
        current_size = None

        for idx, row in df.iterrows():
            value_before = row[first_col]
            value = value_before.replace('"', '').replace('\\', '').replace("\xa0", " ") 
            if value in thread_sizes:
                current_size = value_before
            df.at[idx, 'thread_size'] = current_size

        df = df[~df[first_col].apply(lambda x: x.replace('"', '').replace('\\', '').replace("\xa0", " ") in thread_sizes)].reset_index(drop=True)

        return df

    @staticmethod
    def assign_material_surface(data_dict, dataframe):
        materials = data_dict.get('material_surface', [])
        if not materials:
            return dataframe

        df = dataframe.copy()
        first_col = df.columns[0]
        df['material_surface'] = None
        current_material = None

        for idx, row in df.iterrows():
            value = row[first_col]
            if value in materials:
                current_material = value
            df.at[idx, 'material_surface'] = current_material

        df = df[~df[first_col].isin(materials)].reset_index(drop=True)
        return df

    def merge_header(self, processed_df):
        header_columns = self.header_dfs[0].columns.tolist()
        processed_columns = processed_df.columns.tolist()

        aligned_header = pd.DataFrame(columns=processed_columns)
        for col in header_columns:
            if col in processed_columns:
                aligned_header[col] = self.header_dfs[0].get(col, np.nan)

        full_df = pd.concat([aligned_header, processed_df], ignore_index=True)
        return full_df
def preprocess_for_excel_name(bucket_name, file_key):
    html_elements = clean_html_task(bucket_name, file_key)
    soup = BeautifulSoup(html_elements, "html.parser")
    h3_elements = soup.find_all("h3")
    # print(f"All the h3 elements: {h3_elements}")
    
    last_text = ""
    if h3_elements:
        last_h3 = h3_elements[-1]
        text_parts = list(last_h3.stripped_strings)
        if text_parts:
            last_text = text_parts[-1]
    
    last_text = last_text.replace(" ", "_").replace(".", "")
    return last_text

def save_dataframes_to_s3( dataframes, bucket_name,  file_key):#dataframes,
    # if filename is None:
    base_name = preprocess_for_excel_name(bucket_name, file_key)
    # print(f"Generated base name for Excel file: {base_name}")
    dir_path = "./mcmaster_excel/"
    filename = os.path.join(dir_path, f"{base_name}.xlsx")
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    if not dataframes:
        print("No data to save")
        return

    try:
        with pd.ExcelWriter(filename, engine="openpyxl") as writer:
            for idx, df in enumerate(dataframes, 1):
                if not df.empty:
                    sheet_name = f"Table_{idx}"[:31]  # Sheet names must be <= 31 characters
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    print(f"Skipping empty DataFrame at index {idx}")

        # print(f"Successfully saved to {filename}")

        # Upload to S3
        s3_key = f"parsed_data/{os.path.basename(filename)}"
        s3_client.upload_file(filename, OUTPUT_BUCKET, s3_key)
        print(f"Successfully uploaded {filename} to S3 bucket {OUTPUT_BUCKET} as {s3_key}")

    except Exception as e:
        print(f"Save error: {e}")

@celery_app.task(name="parse_task")
def parse_task(bucket_name, file_key):
    # print(f"Downloading {file_key} from bucket {bucket_name}")
    os.makedirs("./temp", exist_ok=True)
    local_filename = f"./temp/{file_key.split('/')[-1]}"
    try:
        s3_client.download_file(bucket_name, file_key, local_filename)
        # print(f"Downloaded {file_key} to {local_filename}")
    except Exception as e:
        print(f"Failed to download {file_key} from S3: {e}")
        return {"error": f"Failed to download file: {e}"}


    start_time = time.time()

    final_data = FinalDataframe(bucket_name, file_key)
    print(f"Initialized FinalDataframe with {final_data.total_tables} tables")
    final_data.process_all_tables()

    parsed_filename = local_filename.replace(".html", ".xlsx")
    save_dataframes_to_s3(final_data.processed_dfs, bucket_name, file_key)

    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds")
    return f"Processing completed in {end_time - start_time:.2f} seconds"
