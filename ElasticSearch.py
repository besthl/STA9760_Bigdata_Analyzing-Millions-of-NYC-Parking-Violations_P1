from sys import argv
from src.OPCV.getdata import Function
from os import environ
from elasticsearch import Elasticsearch


def create_and_update_index(index_name, doc_type):
    es = Elasticsearch()
    try:
        es.indices.create(index=index_name)
    except Exception:
        pass

    return es

def insert(docs, es):
    for doc in docs:
        res = es.index(index='violation-parking-index', doc_type='vehicle', body=doc, )
        print(res['result'])    


if __name__ == "__main__":
    app_key = environ.get("APP_KEY")
    es = create_and_update_index('violation-parking-index', 'vehicle')
    #print page_size and num_pages
    page_size_str = argv[1]
    page_size = int(page_size_str.split('=')[1])

    try:
        num_pages_str = argv[2]
        num_pages = int(num_pages_str.split('=')[1])
    except Exception:
        num_pages = None
    
    #print output
    try:
        output = argv[3]

    except Exception:

    # print(f"page_size={page_size}, num_pages={num_pages}")
    
        location = 'nc67-uf89'

    with Function(app_key) as function:
        if num_pages is None:
            docs = function.get_info(location, page_size)
            insert(docs, es)
        else:
            total_size = function.get_size(location)
            # print(f"total_size={total_size}")
            docs = function.get_info(location, page_size)
            insert(docs, es)

            for i in range(num_pages):       
                docs = function.get_next_info(location, page_size, offset=i*num_pages)
                insert(docs, es)
