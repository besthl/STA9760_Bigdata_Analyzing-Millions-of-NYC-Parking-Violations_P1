from sys import argv
from src.parkingviolations.api import Service
from os import environ
from elasticsearch import Elasticsearch


def create_and_update_index(index_name, doc_type):
    es = Elasticsearch()
    try:
        es.indices.create(index=index_name)
    except Exception:
        pass

    es.indices.put_mapping(
        index=index_name,
        doc_type=doc_type,
        body={
            doc_type: {
                "properties": {"issue_date": {"type": "date"},
                "fine_amount":{"type":"integer"},
                "penalty_amount":{"type": "integer"},
                "interest_amount":{"type": "integer"},
                "reduction_amount":{"type":"integer"},
                "payment_amount":{"type":"integer"},
                "amount_due":{"type":"integer"}}
            }
        }
    )
    

    return es

def insert(docs, es):
    for doc in docs:
        res = es.index(index='violation-parking-index', doc_type='vehicle', body=doc, )
        print(res['result'])    


if __name__ == "__main__":
    app_key = environ.get("APP_KEY")
    es = create_and_update_index('violation-parking-index', 'vehicle')

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

    es = Elasticsearch()
    location = 'nc67-uf89'

    with Function(app_key) as function:
        if num_pages is None:
            docs = service.get_info(location, page_size)
            insert(docs, es)
        else:
            total_size = service.get_size(location)
            # print(f"total_size={total_size}")
            docs = service.get_info(location, page_size)
            insert(docs, es)

            for i in range(num_pages):       
                docs = service.get_next_info(location, page_size, offset=i*num_pages)
                insert(docs, es)