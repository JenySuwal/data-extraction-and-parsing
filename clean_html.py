from bs4 import BeautifulSoup, Comment, Tag, NavigableString

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



def build_block_tree(html_content, max_words=50):
    cleaned_html = clean_html(html_content)
    soup = BeautifulSoup(cleaned_html, "html.parser")
 

    return str(soup)


sample_html_path = "./data/tables_1742457658.html"


def call_build_block():
    with open(sample_html_path, "r", encoding="utf-8") as file:
        sample_html = file.read()  
    
    
    block_tree_html = build_block_tree(sample_html)

    
    return block_tree_html

call_build_block()