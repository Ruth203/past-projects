import requests
import ast
import time
from multiprocessing.dummy import Pool
#import json

pool = Pool(10) # Creates a pool with ten threads; more threads = more concurrency.
                # "pool" is a module attribute; you can be sure there will only
                # be one of them in your application
                # as modules are cached after initialization.

def get_titles():
    #url = "https://us-central1-mapreduce-369610.cloudfunctions.net/splitter-function"
    url = "https://us-central1-mapreduce-369610.cloudfunctions.net/bk-bucket-function"

    r = requests.get(url=url)

    data = r.text

    #print(data)

    books = ast.literal_eval(data)

    total_books = len(books)
    load = len(books)//20       # work out how to divide batches
    step = total_books//load    # divide into batches

    #print(step)
    divide = [books[i::step] for i in range(step)]

    return divide

def get_words(books):

    #print(divide)
    futures = []

    for title_list in books:
        url = "https://us-central1-mapreduce-369610.cloudfunctions.net/splitter-function"

        PARAMS = {'books':str(title_list)}
        print(PARAMS)

        # r = requests.get(url=url, params=PARAMS)
        # data = r.text
        # print(data)
        
        futures.append(pool.apply_async(requests.get, [url, PARAMS]))

    for future in futures:
        print(future.get()) # For each future, wait until the request is
                            # finished and then print the response object.

    return 'done'

def anagram_mapper():
    url = "https://us-central1-mapreduce-369610.cloudfunctions.net/mapper-function"

    r = requests.get(url=url)
    data = r.text
    print(data)

def shuffle(titles):
    futures = []

    for title in titles:
        url = "https://us-central1-mapreduce-369610.cloudfunctions.net/shuffler-function"

        # r = requests.get(url=url)
        # data = r.text
        # print(data)

        PARAMS = {'books':str(title)}
        print(PARAMS)

        # r = requests.get(url=url, params=PARAMS)
        # data = r.text
        # print(data)
        
        futures.append(pool.apply_async(requests.get, [url, PARAMS]))

    for future in futures:
        print(future.get()) # For each future, wait until the request is
                            # finished and then print the response object.

if __name__== "__main__":

    titles = get_titles()
    start = time.time()
    word_pool = get_words(titles)
    duration = time.time() - start
    print('Pool took: ', duration)

    start = time.time()
    anagram_mapper()
    duration = time.time() - start
    print('Mapper took: ', duration)

    start = time.time()
    shuffle(titles)
    duration = time.time() - start
    print('Shuffler took: ', duration)
