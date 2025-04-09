from clean_html import clean_html_task
from bs4 import BeautifulSoup
import pandas as pd

def create_dataframes(bucket_name, file_key):

    html_data = clean_html_task(bucket_name, file_key)  
    soup = BeautifulSoup(html_data, "html.parser")
    tables = soup.find_all("table")

    thead_dataframes = []
    tbody_dataframes = []

    for table in tables:
        thead_data = []
        tbody_data = []

        # thead = table.find("thead")
        # if thead:
        #     for row in thead.find_all("tr"):
        #         cols = row.find_all(["th", "td"])
        #         thead_data.append([col.get_text(strip=True) for col in cols])

        # tbody = table.find("tbody")
        thead = table.find("thead")
        # print(thead)
        if thead:
            for row in thead.find_all("tr"):
                cols = row.find_all(["th", "td"])
                # Convert <td> to <th> inside <thead>
                for col in cols:
                    if col.name == "td":
                        col.name = "th"
                thead_data.append([col.get_text(strip=True) for col in cols])
            # print(f"Processed thead: {thead_data}")
 
        tbody = table.find("tbody")
        if tbody:
            orphan_ths = tbody.find_all("th", recursive=False)  
            if orphan_ths:
                for orphan_th in orphan_ths:
                    new_td = soup.new_tag("td")
                    new_td.string = orphan_th.get_text(strip=True)
                    new_tr = soup.new_tag("tr")
                    new_tr.append(new_td)
                    orphan_th.insert_before(new_tr)
                    orphan_th.decompose()
            
            for row in tbody.find_all("tr"):
                cols = row.find_all(["th", "td"])
                tbody_data.append([col.get_text(strip=True) for col in cols])

        max_thead_cols = max((len(row) for row in thead_data), default=0)
        max_tbody_cols = max((len(row) for row in tbody_data), default=0)
        
        thead_df = pd.DataFrame([row + [""] * (max_thead_cols - len(row)) for row in thead_data])
        tbody_df = pd.DataFrame([row + [""] * (max_tbody_cols - len(row)) for row in tbody_data])
        # print(thead_df)
        
        thead_dataframes.append(thead_df)
        tbody_dataframes.append(tbody_df)
        # print(f"Processed table with {(thead_data)} header rows and {(tbody_data)} body rows.")
    return thead_dataframes, tbody_dataframes

