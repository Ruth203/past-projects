import base64
import ast
from google.cloud import storage
from google.cloud.storage import Blob

client = storage.Client()

bucket = None


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    global client

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    dictionary(pubsub_message)

def dictionary(message):
     print(type(message))

     # Converts to list
     message = ast.literal_eval(message)
     print(type(message), message)

     # Converts to dict
     message = ast.literal_eval(str(message[0]))
     print(type(message), message)

     all_anagrams = message.keys()

     for anagram in all_anagrams:
          # Remove duplicates and add to new dictionary
          final_dict = dict()
          print('Anagram: ', anagram)
          values = message[anagram]
          print('Initial values: ', values)
          values = set(values)
          final_dict[anagram] = list(values)
          print('New value in dict: ', final_dict)
          # Store each anagram as object in bucket
          store(anagram, final_dict)

def store(anagram, anagram_dict):
    """ Store in single bucket to be checked later if no immediate match otherwise store in final results bucket"""
    global bucket
    if not bucket:
        results_bucket = 'results-mapreduce-369610'
        bucket = client.bucket(results_bucket)

    single_bucket = 'single-results-mapreduce-369610'
    temp_bucket = client.bucket(single_bucket)

    single = True

    # check singles bucket first for a match
    for temp in temp_bucket.list_blobs():
        anagram_name = temp.name
        
        if anagram_name == anagram:
            single = False
            anagram_concat(anagram, anagram_dict)
            temp.delete()

    if single:
        # check valid sets
        for blob in bucket.list_blobs():
            anagram_name = blob.name
            
            if anagram_name == anagram:
                anagram_concat(anagram, anagram_dict)
    
    if single and len(anagram_dict[anagram]) < 2:
        # Upload blob to single bucket
        upload_blob = Blob(anagram, temp_bucket)
        # Upload
        upload_blob.upload_from_string(str(anagram_dict), content_type='text/plain')

    elif single:
        # Upload blob for anagram
        upload_blob = Blob(anagram, bucket)
        # Upload
        upload_blob.upload_from_string(str(anagram_dict), content_type='text/plain')

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
        results_bucket = 'results-mapreduce-369610'
        bucket = client.bucket(results_bucket)

    words = ""
    blob = Blob(bk, bucket)
    try:
        words = blob.download_as_string().decode("utf-8") + " "
    except:
        words = blob.download_as_string().decode("latin-1") + " "

    # convert str to dict
    words = ast.literal_eval(words)

    return words

def anagram_concat(anagram, anagram_dict):
    """make anagrams into one list"""
    global bucket
    if not bucket:
        results_bucket = 'results-mapreduce-369610'
        bucket = client.bucket(results_bucket)
        
    existing_content = download_blob(anagram)
    new_value = set()
    for i in existing_content[anagram]:
        new_value.add(i)
    for i in anagram_dict[anagram]: 
        new_value.add(i)
    new_value = list(new_value)
    new_value = new_value.sort()
    anagram_dict[anagram] = new_value

    # Upload and overwrite pre-existing blob for anagram
    upload_blob = Blob(anagram, bucket)
    # Upload
    upload_blob.upload_from_string(str(anagram_dict), content_type='text/plain')
