from google.cloud import storage
from google.cloud.storage import Blob
import string
import ast
from google.cloud import pubsub_v1
import requests


# Get book contents from pool-mapreduce-369610

project_id = 'mapreduce-369610'

client = storage.Client()

bucket = None

publisher = None

this_dict = dict()

def shuffle(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    global project_id
    global client
    #global this_dict

    if request.args:
        books = f"{request.args.get('books')}"
        print('args: ', books)

        # Start process
        alpha_dict = get_words(books)

        # Publish to topic - topic corresponds to each alphabetical letter
        publish(alpha_dict)

    return 'shuffler done.'


def get_words(title):

    # global bucket
    # if not bucket:
    #     anagrams_bucket = 'anagrams-mapreduce-369610'
    #     bucket = client.bucket(anagrams_bucket)

    # for blob in bucket.list_blobs():
    #     bk_name = blob.name
    #     print(bk_name)
 
    # get content of blob (all unique words in the book) and return as a list
    anagram_dict = download_blob(title)

    # Get nested dictionary where anagram's starting letter is primary key (1 book)
    alpha_dict = get_keys(anagram_dict)

    return alpha_dict


def download_blob(bk):
    """
    Get content of all passed blobs and add to a str.
        
        args:
            bl - blob name
            self.bucket - name of bucket blob bl belongs to
            
        returns:
            string of all passed blob content
            
    """
    global bucket

    if not bucket:
        anagrams_bucket = 'anagrams-mapreduce-369610'
        bucket = client.bucket(anagrams_bucket)

    words = ""
    blob = Blob(bk, bucket)
    try:
        words = blob.download_as_string().decode("utf-8") + " "
    except:
        words = blob.download_as_string().decode("latin-1") + " "

    # convert str to dict
    words = ast.literal_eval(words)

    return words

def get_keys(ana_dict):
    """Return a list of all keys in dictionary and sort into another dictionary that uses the alphabet as keys 
    (use starting letter of anagram to decide key."""
    # global publisher

    # if not publisher:
    #     publisher = pubsub_v1.PublisherClient()

    all_keys = ana_dict.keys()
    
    print('alpha_dict = ', this_dict)

    for key in all_keys:
        
        pair = dict()
        pair[key] = ana_dict[key]
        
        if key[0] not in this_dict:
            this_dict[key[0]] = list()
            # add key-value pair to correct alphabet key
            this_dict[key[0]].append(pair)
        else:
            existing_dict = this_dict[key[0]][0]
            #print('Existing dict: ', existing_dict)
            new_dict = {**existing_dict, **pair}
            for key, value in new_dict.items():
                if key in existing_dict and key in pair:
                    # print('value: ', value)
                    # print('existing_dict: ', existing_dict[key])
                    new_dict[key] = [value[0] , existing_dict[key][0]]
            #print('Merged dict: ', new_dict)
            this_dict[key[0]][0] = new_dict

    #return alpha_dict
    return this_dict