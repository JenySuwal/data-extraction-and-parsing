import boto3
from bs4 import BeautifulSoup, Comment, Tag, NavigableString

s3_client = boto3.client('s3')

def clean_html(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    void_elements = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 
                     'link', 'meta', 'param', 'source', 'track', 'wbr'}

    for tag in soup.find_all(void_elements):
        tag.decompose()  
    for br in soup.find_all("br"):
        br.replace_with(" ")

    def preserve_text_before_removal(tag):
        if any(child.name and child.name.startswith("h") for child in tag.find_all()):
            return
        if tag.name == "a":
            tag.replace_with(NavigableString(tag.get_text(" ", strip=True)))
        elif tag.contents:
            tag.replace_with(NavigableString(tag.get_text(" ", strip=True)))

    for tag in soup.find_all(["div", "span", "a"]):
        # if tag.name in ["thead", "table"]:
        #     continue
        # if not tag.get_text(strip=True):  
        #     tag.extract()
        # else:
        #     preserve_text_before_removal(tag)
        if not tag.get_text(strip=True):  
            tag.extract()
        else:
            preserve_text_before_removal(tag)

    for tag in soup.find_all(True):
        if tag.name in ["td", "th"]:
            tag.attrs = {k: v for k, v in tag.attrs.items() if k in ["colspan", "rowspan"]}
        else:
            tag.attrs = {}  

    def merge_single_nested_tags(tag):
        while len(tag.contents) == 1 and isinstance(tag.contents[0], Tag):
            inner_tag = tag.contents[0]
            tag.replace_with(inner_tag)
            tag = inner_tag

    for tag in soup.find_all(True):
        merge_single_nested_tags(tag)

    return str(soup)
# def ensure_thead_between_table_and_tbody(html: str) -> str:
#     soup = BeautifulSoup(html, 'html.parser')
 
#     for table in soup.find_all("table"):
#         tbody = table.find("tbody")
#         thead = table.find("thead")
 
#         # If <tbody> exists and <thead> does not
#         if tbody and not thead:
#             new_thead = soup.new_tag("thead")
#             tbody.insert_before(new_thead)
 
#     return str(soup)
def ensure_thead_between_table_and_tbody(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')
 
    for table in soup.find_all("table"):
        tbody = table.find("tbody")
        thead = table.find("thead")
 
        if tbody and not thead:
            new_thead = soup.new_tag("thead")
            tbody.insert_before(new_thead)
           
            for tr in table.find_all('tr', recursive=False):
                tr.extract()
                new_thead.append(tr)
 
    return str(soup)

def move_orphan_ths_to_tbody(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')

    for table in soup.find_all('table'):
        thead = table.find('thead')
        tbody = table.find('tbody')

        if not thead or not tbody:
            continue


        orphan_ths = [child for child in thead.children 
                     if isinstance(child, Tag) and child.name == 'th']

        for th in orphan_ths:

            new_tr = soup.new_tag('tr')
            th.wrap(new_tr)
            

            tbody.insert(0, new_tr)

   
        if not thead.find_all(True): 
            thead.decompose()

    return str(soup)

def ensure_tbody_after_thead(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')

    for table in soup.find_all("table"):
        thead = table.find("thead")
        tbody = table.find("tbody")

        if not tbody:
            tbody = soup.new_tag("tbody")

        if thead:
            
            if tbody not in table.contents:
                thead.insert_after(tbody)
            elif table.contents.index(tbody) < table.contents.index(thead):
               
                tbody.extract()
                thead.insert_after(tbody)
        else:
            
            if tbody not in table.contents:
                table.insert(0, tbody)

    return str(soup)

def multiple_tbody_in_table(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
 
    for table in tables:
        tbodies = table.find_all("tbody")
 
        if len(tbodies) > 1:
           
           
            for tbody in tbodies:
                for td in tbody.find_all("td"):
                    colspan = td.get("colspan")
                    if colspan and int(colspan) > 5:
                        th = soup.new_tag("th", **td.attrs)
                        th.string = td.get_text(strip=True)
                        td.replace_with(th)
 
           
            first_tbody = tbodies[0]
 
           
            for tbody in tbodies[1:]:
                for element in tbody.find_all(["tr", "th"]):
                    first_tbody.append(element)
 
           
            for tbody in tbodies[1:]:
                tbody.unwrap()  
 
    return str(soup)
def split_tbodies_into_tables(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for table in soup.find_all("table"):
        tbodies = table.find_all("tbody", recursive=False)

        if len(tbodies) <= 1:
            continue

        new_tables = []
        for tbody in tbodies:
            new_table = soup.new_tag("table")
            tbody.extract()
            new_table.append(tbody)
            new_tables.append(new_table)

        table.insert_after(*new_tables)
        table.decompose()

    return str(soup)

def merge_incomplete_rows(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        i = 0
        while i < len(rows) - 1:
            td_list = rows[i].find_all("td")
            if len(td_list) == 2 and not td_list[1].get_text(strip=True):
                next_td_list = rows[i+1].find_all("td")
                if len(next_td_list) == 2:
                    td_list[0].string = td_list[0].get_text(strip=True) + ' ' + next_td_list[0].get_text(strip=True)
                    td_list[1].string = next_td_list[1].get_text(strip=True)
                    rows[i+1].decompose()
                    rows = table.find_all("tr")
                    continue
            i += 1

    return str(soup)

def process_and_clean_html(html_content: str) -> str:
    cleaned_html = clean_html(html_content)
    fixed_html = move_orphan_ths_to_tbody(cleaned_html)
    fixed_html = ensure_thead_between_table_and_tbody(fixed_html)
     
    fixed_html = ensure_tbody_after_thead(fixed_html)

    fixed_html = split_tbodies_into_tables(fixed_html)
    fixed_html = merge_incomplete_rows(fixed_html)
    fully_processed_html = multiple_tbody_in_table(fixed_html)
    
    return fully_processed_html

def build_block_tree(html_content, max_words=50):
    cleaned_html = clean_html(html_content)
    fixed_html = ensure_thead_between_table_and_tbody(cleaned_html)
    fixed_html=ensure_tbody_after_thead(fixed_html)
    # soup = BeautifulSoup(fixed_html, "html.parser")
    fully_processed_html = multiple_tbody_in_table(fixed_html)
    return fully_processed_html

    # return str(soup)


import os
# Function to download and clean HTML file from S3
def clean_html_task(bucket_name, file_key):
    local_path = f"./temp/{file_key.split('/')[-1]}"
    if not os.path.exists(local_path):
        # print(f"File not found locally, downloading from S3: {file_key}")
        s3_client.download_file(bucket_name, file_key, local_path)

    try:
        with open(local_path, 'r', encoding='utf-8') as f:
            sample_html = f.read()
    except Exception as e:
        return {"error": f"Failed to read local file: {e}"}

    # Clean the HTML
    block_tree_html = build_block_tree(sample_html)
    # cleaned_html = clean_html(sample_html)
    # print(f"Cleaned HTML length: {block_tree_html} characters")
    return block_tree_html
