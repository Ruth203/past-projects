from google.cloud import storage
from google.cloud.storage import Blob
import string
import requests

client = storage.Client()

# The OG book bucket
bucket = None

anagram_bucket = None

def hello_world(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    
    global client

    get_words()

    return('mapper done.')
    

def get_words():

    ## Go through word retrieval and storage book by book ##
    print('get words')

    global bucket

    if not bucket:
        pool_bucket = 'pool-mapreduce-369610'
        bucket = client.bucket(pool_bucket)

    blob_list = []
    for blob in bucket.list_blobs():
        bk_name = blob.name
        blob_list.append(bk_name)
          
        # get content of blob (all unique words in the book) and return as a list
        word_list = download_blob(bk_name)

        # Create key-value pairs from list
        anagram_dict = key_value_dict(word_list)

        # Store in bucket
        store(anagram_dict, bk_name)


def download_blob(bk):
    """
    Get content of all passed blobs and add to a str.
          
        args:
            bl - blob name
            self.bucket - name of bucket blob bl belongs to
              
        returns:
            string of all passed blob content
              
    """
    print('download blob')
    global bucket

    if not bucket:
        pool_bucket = 'pool-mapreduce-369610'
        bucket = client.bucket(pool_bucket)

    words = ""
    blob = Blob(bk, bucket)
    try:
        words = blob.download_as_string().decode("utf-8") + " "
    except:
        words = blob.download_as_string().decode("latin-1") + " "

    return words.split()


def key_value_dict(words):
    """
    For each word in list, determine its anagram and add to dictionary with the original word as the value.
    """
    print('key value dict')
    key_values = dict()

    for word in words:
        anagram = ''.join(sorted(word))

        # allow for multiple words to one anagram (key)
        if anagram not in key_values:
              key_values[anagram] = list()    # don't need to worry about duplicates thanks to the splitter so no set required
        
        # add value to the corresponding anagram key
        key_values[anagram].extend([word])

    return key_values


def store(anagram_dict, bk):
     """
     For each book, store the anagram dictionary as a single object.

     Should think about storing this in a smart way to help with shuffling later.
     """
     print('store')
     global anagram_bucket

     if not anagram_bucket:
          ana_bucket = 'anagrams-mapreduce-369610'
          anagram_bucket = client.bucket(ana_bucket)

     # Create object wrapper
     upload_blob = Blob(bk, anagram_bucket)
     # Upload
     upload_blob.upload_from_string(str(anagram_dict), content_type='text/plain')
