from sys import argv
from src.OPCV.getdata import Function
from os import environ
from math import ceil

if __name__ == "__main__":
    app_key = environ.get("APP_KEY")
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
        output = None
    location = 'nc67-uf89'

    #limit_size = int(page_size / num_pages)

    if output is None:
        with Function(app_key) as function:
            if num_pages == None: 
                total_size = function.get_size(location)
                num_pages = ceil(total_size / page_size)
            print (f'num_pages = {num_pages}')
            print (f'page_size = {page_size}')
            h = input("Press 'Enter' to continue...")

            for i in range(num_pages):
                print(function.get_info(location, page_size, offset=i*num_pages))
    else:
        output = output.split("=")[1]
        with Function(app_key) as function, open(output, "w") as fw:
            if num_pages == None: 
                total_size = function.get_size(location)
                num_pages = ceil(total_size / page_size)
            print (f'num_pages = {num_pages}')
            print (f'page_size = {page_size}')
            h = input("Press 'Enter' to continue...")

            for i in range(num_pages):
                fw.write(f"{function.get_info(location, page_size, offset=i*num_pages)}\n")
