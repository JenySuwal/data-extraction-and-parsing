from celery import Celery
import json
import time
import redis
import pandas as pd
import boto3
import numpy as np
from clean_html import clean_html_task
from bs4 import BeautifulSoup
from urllib.parse import urlparse
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
            try:
                header_df = self.header_dfs[i].copy()
                body_df = self.body_dfs[i]
                for i in range(len(self.llm_output_list)):
                    llm_output = self.llm_output_list[i]
            except IndexError as e:
                print(f"[IndexError] At table index {i}:")
                print(f" - total_tables: {self.total_tables}")
                print(f" - header_dfs: {len(self.header_dfs)}")
                print(f" - body_dfs: {len(self.body_dfs)}")
                print(f" - llm_output_list: {len(self.llm_output_list)}")
                continue  # or `break` if you want to stop execution

            merged_df = self.add_header(llm_output, header_df, body_df)
            if merged_df is not None:
                try:
                    data_dict = json.loads(llm_output)
                    merged_df = self.assign_seal_type(data_dict, merged_df)
                    merged_df = self.assign_material_surface(data_dict, merged_df)
                    final_df = self.merge_header(header_df, merged_df)
                    self.processed_dfs.append(final_df)
                except Exception as e:
                    print(f"[ProcessingError] at table {i+1}: {e}")
            else:
                print(f"Skipping table {i+1} due to processing error.")


    @staticmethod
    def add_header(llm_output, header_df, body_df):
        try:
            parsed_dict = json.loads(llm_output)
            keys = list(parsed_dict.keys())

            head_row_len = header_df.shape[0]
            no_cols = header_df.shape[1]

            
            for i, key in enumerate(keys):
                new_column_name = str(no_cols + 1)
                header_df[new_column_name] = np.nan
                header_df[new_column_name] = header_df[new_column_name].astype('object')
                header_df.loc[head_row_len - 1, new_column_name] = key
                no_cols += 1

            
            combined = pd.concat([header_df.iloc[[-1]], body_df], ignore_index=True)
            combined.columns = combined.iloc[0]  
            combined = combined.drop(0).reset_index(drop=True)  
            combined.columns = combined.columns.str.strip()  

            return combined
        except Exception as e:
            print(f"Error adding header: {e}")
            return None

    # #########################################works for Openai API####################################################### 
    # @staticmethod ########works for screw category
    # def assign_thread_size(data_dict, dataframe):
    #     thread_sizes = set()
    #     def clean_value(value):           
    #         return ''.join(
    #             str(value)
    #                 .replace('"', '')
    #                 .replace('\\', '')
    #                 .replace("\xa0", " ")
    #                 .replace(' ', '')  
    #         )  
    #     for item in data_dict.get('thread_size', []):  
    #         try:
    #             cleaned_item = clean_value(item)
    #             thread_sizes.add(cleaned_item)    
    #         except Exception as e:
    #             print(f"Error processing item: {item} - {e}")
    #     if not thread_sizes:
    #         return dataframe
    #     df = dataframe.copy()
    #     df.columns = [f"{col}_{i}" if col in df.columns[:i] else col for i, col in enumerate(df.columns)]
    #     first_col = df.columns[0]
    #     df['thread_size'] = None  
    #     current_size = None
    #     for idx, row in df.iterrows():
    #         value_before = row[first_col]
    #         cleaned_value = clean_value(value_before)
    #         if cleaned_value in thread_sizes:
    #             current_size = value_before  
    #         df.at[idx, 'thread_size'] = current_size
    #     mask = df[first_col].map(lambda x: clean_value(x) in thread_sizes)
    #     df = df[~mask].reset_index(drop=True)
    #     return df
    @staticmethod
    def assign_seal_type(data_dict, dataframe):
        seal_types = set()

        def clean_value(value):
            
            return ''.join(
                str(value)
                    .replace('"', '')
                    .replace('\\', '')
                    .replace("\xa0", " ")
                    .replace(' ', '')
            )     
        for item in data_dict.get('seal_type', []):
            try:
                cleaned_item = clean_value(item)
                seal_types.add(cleaned_item)
            except Exception as e:
                print(f"Error processing item: {item} - {e}")
        if not seal_types:
            return dataframe
        df = dataframe.copy()
        df.columns = [f"{col}_{i}" if col in df.columns[:i] else col for i, col in enumerate(df.columns)]

        first_col = df.columns[0]
        df['seal_type'] = None
        current_seal = None

        for idx, row in df.iterrows():
            original_value = row[first_col]
            cleaned_value = clean_value(original_value)
            if cleaned_value in seal_types:
                current_seal = original_value  
            df.at[idx, 'seal_type'] = current_seal  
        mask = df[first_col].apply(lambda x: clean_value(x) in seal_types)
        df = df[~mask].reset_index(drop=True)
        return df

    @staticmethod
    def assign_material_surface(data_dict, dataframe):
        materials = set()

        def clean_value(value):
            return ''.join(
                str(value)
                    .replace('"', '')
                    .replace('\\', '')
                    .replace("\xa0", " ")
                    .replace(' ', '')
            )

        
        for item in data_dict.get('material_surface', []):
            try:
                cleaned_item = clean_value(item)
                materials.add(cleaned_item)
            except Exception as e:
                print(f"Error processing item: {item} - {e}")
        if not materials:
            return dataframe
        df = dataframe.copy()
        df.columns = [f"{col}_{i}" if col in df.columns[:i] else col for i, col in enumerate(df.columns)]

        first_col = df.columns[0]
        df['material_surface'] = None
        current_material = None

        for idx, row in df.iterrows():
            original_value = row[first_col]
            cleaned_value = clean_value(original_value)
            if cleaned_value in materials:
                current_material = original_value  
            df.at[idx, 'material_surface'] = current_material
        mask = df[first_col].apply(lambda x: clean_value(x) in materials)
        df = df[~mask].reset_index(drop=True)
        return df


    # @staticmethod
    # def assign_material_surface(data_dict, dataframe):
        
    #     materials = set()

    #     def clean_value(value):
            
    #         return ''.join(
    #             str(value)
    #                 .replace('"', '')
    #                 .replace('\\', '')
    #                 .replace("\xa0", " ")
    #                 .replace(' ', '')  
    #         )    
    #     for item in data_dict.get('material_surface', []):           
    #         try:  
    #             cleaned_item = clean_value(item)
    #             materials.add(cleaned_item)
    #         except Exception as e:
    #             print(f"Error processing item: {item} - {e}")      
    #     if not materials:
    #         return dataframe     
    #     df = dataframe.copy()
    #     df.columns = [f"{col}_{i}" if col in df.columns[:i] else col for i, col in enumerate(df.columns)]

    #     first_col = df.columns[0]
    #     df['material_surface'] = None
    #     current_size = None  

    #     for idx, row in df.iterrows():
    #         value_before = row[first_col]
    #         cleaned_value = clean_value(value_before)
    #         if cleaned_value in materials:
    #             current_size = value_before  
    #         df.at[idx, 'material_surface'] = current_size 
    #     mask = df[first_col].map(lambda x: clean_value(x) in materials)   
    #     df = df[~mask].reset_index(drop=True)     
    #     return df
    

    @staticmethod
    def merge_header(header_df, processed_df):
        try:
            header_columns = header_df.columns.tolist()
            assign_columns = processed_df.columns.tolist()
            column_mapping = {header_col: assign_col for header_col, assign_col in zip(header_columns, assign_columns)}
            header_df_renamed = header_df.rename(columns=column_mapping)
            merged_df = pd.concat([header_df_renamed, processed_df], axis=0, ignore_index=True)
            merged_df.columns = range(len(merged_df.columns))

            return merged_df
        except Exception as e:
            print(f"Error merging header: {e}")
            return None
   #########################################works for Openai API####################################################### 

# from urllib.parse import urlparse
# from bs4 import BeautifulSoup
# import re

# def extract_url_parts(url):
#     """Extract and return different parts of a URL as a dictionary"""
#     parsed = urlparse(url)
#     path_parts = [part for part in parsed.path.split('/') if part]
    
#     return {
#         'netloc': parsed.netloc,
#         'path': parsed.path,
#         'path_parts': path_parts,
#         'last_part': path_parts[-1] if path_parts else '',
#         'second_last_part': path_parts[-2] if len(path_parts) > 1 else '',
#         'full_url': url
#     }

# def preprocess_for_excel_name(bucket_name, file_key, url=None):
#     """Generate Excel filename from URL instead of HTML content"""
#     if not url:
#         # Fallback to old behavior if URL isn't provided
#         html_elements = clean_html_task(bucket_name, file_key)
#         soup = BeautifulSoup(html_elements, "html.parser")
#         h1 = soup.find("h1")
#         return h1.get_text().strip().replace(" ", "_") if h1 else "data"
    
#     # Process URL to get filename
#     url_parts = extract_url_parts(url)
    
#     # Clean the last part of the path to make it filename-friendly
#     last_part = url_parts['last_part']
    
#     # Remove common URL suffixes and special characters
#     filename = re.sub(r'[~\-/\d]+$', '', last_part)  # Remove trailing numbers, ~, -
#     filename = re.sub(r'[^\w\-_]', '_', filename)    # Replace special chars with underscore
#     filename = filename.strip('_')                   # Remove leading/trailing underscores
    
#     # If we ended up with nothing, use the second last part
#     if not filename and len(url_parts['path_parts']) > 1:
#         filename = url_parts['second_last_part']
#         filename = re.sub(r'[^\w\-_]', '_', filename)
#         filename = filename.strip('_')
    
#     # Final fallback if we still have nothing
#     if not filename:
#         filename = "extracted_data"
    
#     return filename.lower()  


# def save_dataframes_to_s3( dataframes, bucket_name,  file_key):
#     base_name = preprocess_for_excel_name(bucket_name, file_key)
#     dir_path = "./mcmaster_excel/"
#     filename = os.path.join(dir_path, f"{base_name}.xlsx")
#     os.makedirs(os.path.dirname(filename), exist_ok=True)

#     if not dataframes:
#         print("No data to save")
#         return

#     try:
#         with pd.ExcelWriter(filename, engine="openpyxl") as writer:
#             for idx, df in enumerate(dataframes, 1):
#                 if not df.empty:
#                     sheet_name = f"Table_{idx}"[:31]  # Sheet names must be <= 31 characters
#                     df.to_excel(writer, sheet_name=sheet_name, index=False)
#                 else:
#                     print(f"Skipping empty DataFrame at index {idx}")

#         s3_key = f"rounded-remaining/{os.path.basename(filename)}"
        
#         s3_client.upload_file(filename, OUTPUT_BUCKET, s3_key)
#         print(f"Successfully uploaded {filename} to S3 bucket {OUTPUT_BUCKET} as {s3_key}")

#     except Exception as e:
#         print(f"Save error: {e}")
def extract_url_parts(url):
    """Extract and return different parts of a URL as a dictionary"""
    if url is None:
        return {
            'netloc': '',
            'path': '',
            'path_parts': [],
            'last_part': 'unknown',
            'second_last_part': '',
            'third_last_part': '',
            'full_url': ''
        }
    
    if isinstance(url, bytes):
        url = url.decode('utf-8')
    elif not isinstance(url, str):
        url = str(url)  # Convert numbers/other types to string
    
    parsed = urlparse(url)
    path_parts = [part for part in parsed.path.split('/') if part]
    
    return {
        'netloc': parsed.netloc,
        'path': parsed.path,
        'path_parts': path_parts,
        'last_part': path_parts[-1] if path_parts else 'unknown',
        'second_last_part': path_parts[-2] if len(path_parts) > 1 else '',
        'third_last_part': path_parts[-3] if len(path_parts) > 2 else '',
        'full_url': url
    }
import re
def create_folder_structure(url):
    """Creates consistent 3-level structure for McMaster URLs"""
    parts = extract_url_parts(url)
    path_parts = parts['path_parts']
    
    # If URL is invalid/None, use default structure
    if not path_parts:
        return {
            'main_folder': 'unknown',
            'sub_folder': 'unknown',
            'file_name': 'data.xlsx',
            'full_path': 'unknown/unknown/data.xlsx'
        }
    
    # Clean each part (remove numbers/special chars)
    def clean(name):
        name = re.sub(r'[\d~-]+$', '', name)  # Remove trailing numbers/~
        name = re.sub(r'[^\w-]+', '_', name)  # Replace special chars
        return name.strip('_-').lower()
    
    main_folder = clean(path_parts[1]) if len(path_parts) > 1 else 'unknown'
    sub_folder = clean(path_parts[2]) if len(path_parts) > 2 else 'unknown'
    file_name = clean(path_parts[-1]) if path_parts else 'data.xlsx'
    
    return {
        'main_folder': main_folder,
        'sub_folder': sub_folder,
        'file_name': f"{file_name}.xlsx",
        'full_path': f"{main_folder}/{sub_folder}/{file_name}.xlsx"
    }
def save_dataframes_to_s3(dataframes, bucket_name, file_key, url=None):
    """Save dataframes to S3 with consistent folder structure"""
    if not dataframes:
        print("No data to save")
        return

    # Create folder structure (handles None URLs)
    structure = create_folder_structure(url)
    
    # Local paths
    local_dir = "./mcmaster_excel/"
    local_full_path = os.path.join(local_dir, structure['main_folder'], structure['sub_folder'])
    os.makedirs(local_full_path, exist_ok=True)
    
    # Excel file path
    local_filename = os.path.join(local_full_path, structure['file_name'])
    
    try:
        with pd.ExcelWriter(local_filename, engine="openpyxl") as writer:
            for idx, df in enumerate(dataframes, 1):
                if not df.empty:
                    sheet_name = f"Table_{idx}"[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    print(f"Skipping empty DataFrame at index {idx}")

        # S3 path maintains the same structure
        s3_key = structure['full_path']
        s3_client.upload_file(local_filename, bucket_name, s3_key)
        print(f"Successfully uploaded to {s3_key}")

    except Exception as e:
        print(f"Save error: {e}")
@celery_app.task(name="parse_task")
def parse_task(bucket_name, file_key, url=None):
    if url is not None:
        if isinstance(url, bytes):
            url = url.decode('utf-8')
        elif not isinstance(url, str):
            url = str(url) 
    os.makedirs("./temp", exist_ok=True)
    local_filename = f"./temp/{file_key.split('/')[-1]}"
    try:
        s3_client.download_file(bucket_name, file_key, local_filename)
    except Exception as e:
        print(f"Failed to download {file_key} from S3: {e}")
        return {"error": f"Failed to download file: {e}"}
    start_time = time.time()
    final_data = FinalDataframe(bucket_name, file_key)
    final_data.process_all_tables()
    # parsed_filename = local_filename.replace(".html", ".xlsx")
    save_dataframes_to_s3(final_data.processed_dfs, bucket_name, file_key, url)
    end_time = time.time()
    print(f"Processing completed in {end_time - start_time:.2f} seconds")
    return f"Processing completed in {end_time - start_time:.2f} seconds"
