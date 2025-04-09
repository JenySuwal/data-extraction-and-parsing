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

    def preserve_text_before_removal(tag):
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
def build_block_tree(html_content, max_words=50):
    cleaned_html = clean_html(html_content)
    fixed_html = ensure_thead_between_table_and_tbody(cleaned_html)
    fixed_html=ensure_tbody_after_thead(fixed_html)
    soup = BeautifulSoup(fixed_html, "html.parser")
    

    return str(soup)


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
