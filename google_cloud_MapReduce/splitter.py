from google.cloud import storage
from google.cloud.storage import Blob
import string
import requests
import ast

# probably should store this somewhere as a separate entity but rn I don't care
stop_words = ["'tis", "'twas", "a", "able", "about", "across", "after", "ain't", "all", "almost", "also", "am", "among", "an", "and", "any", 
"are", "aren't", "as", "at", "be", "because", "been", "but", "by", "can", "can't", "cannot", "could", "could've", "couldn't", "dear", "did", 
"didn't", "do", "does", "doesn't", "don't", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "hasn't", "have", 
"he", "he'd", "he'll", "he's", "her", "hers", "him", "his", "how", "how'd", "how'll", "how's", "however", "i", "i'd", "i'll", "i'm", "i've", 
"if", "in", "into", "is", "isn't", "it", "it's", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "might've", "mightn't", 
"most", "must", "must've", "mustn't", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", 
"rather", "said", "say", "says", "shan't", "she", "she'd", "she'll", "she's", "should", "should've", "shouldn't", "since", "so", "some", "than", 
"that", "that'll", "that's", "the", "their", "them", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", 
"this", "tis", "to", "too", "twas", "us", "wants", "was", "wasn't", "we", "we'd", "we'll", "we're", "were", "weren't", "what", "what'd", 
"what's", "when", "when", "when'd", "when'll", "when's", "where", "where'd", "where'll", "where's", "which", "while", "who", "who'd", "who'll", 
"who's", "whom", "why", "why'd", "why'll", "why's", "will", "with", "won't", "would", "would've", "wouldn't", "yet", "you", "you'd", "you'll", 
"you're", "you've", "your"]

# The OG book bucket
bucket = None

word_bucket = None


def run(request):
    """Start job"""

    if request.args:
        books = f"{request.args.get('books')}"
        print('args: ', books)

        # convert to list
        books = ast.literal_eval(books)

        print('Processing books...')
        get_book_names(books)

        return f'Splitter done'
    else:
        return f'Splitter failed to get book titles passed'


## List all objects (blobs) in a bucket ##
def get_book_names(books):
    """ 
    Retrieve all book content into string
    Parse string to remove punctuation, stop words etc.
    Store string of parsed book contents.
    """

    group = ""

    for title in books:
        # get content of all blobs
        blob_str = download_blob_str(title)

        group = group + blob_str + " "

    # parse words (remove punctuation, stop words and duplicates)
    word_pool = parse(blob_str)

    # send to bucket as a word pool
    store(word_pool, str(books))
    
    #return blob_list


def download_blob_str(bl):
    """Download content of object (blob) as str
    
    args:
        bl - blob name
        bucket - name of bucket blob bl belongs to
        
    returns:
        string of all passed blob content
        
    """
    global bucket
    if not bucket:
        books_bucket = "coc105-gutenburg-5000books"
        client = storage.Client()
        book_bucket = client.bucket(books_bucket)
    
    words = ""
    blob = Blob(bl, book_bucket)
    try:
        words += blob.download_as_string().decode("utf-8") + " "
    except:
        words += blob.download_as_string().decode("latin-1") + " "

    return words


def parse(word_str):
    """Parse str(s) for '\r \n', punctuation, stop word and duplicate removals."""
    
    # remove \r and \n
    word_str = word_str.replace("\\r\\n", " ")

    # make lower case
    word_str = word_str.lower()

    # remove stop words
    query_words = word_str.split()
    list_words  = [word for word in query_words if word.lower() not in stop_words]
    all_words = ' '.join(list_words)

    # remove punctuation
    table = str.maketrans(string.punctuation, ' '*len(string.punctuation))
    all_words = ' '.join(all_words.translate(table).split())

    # remove words containing numbers
    all_words = ' '.join(s for s in all_words.split() if not any(c.isdigit() for c in s))
    
    # remove duplicates (could potentially make this the mapper's responsibility using sets for adding values)
    words = all_words.split()
    result = " ".join(list(dict.fromkeys(words)))

    return result


def store(pool, book_name):
    """Store words in pool bucket."""
    # Initialise lazy global
    global word_bucket
    
    if not word_bucket:
        words_bucket = "pool-mapreduce-369610"
        client = storage.Client()
        word_bucket = client.bucket(words_bucket)

    # Create object wrapper
    upload_blob = Blob(book_name, word_bucket)
    # Upload
    upload_blob.upload_from_string(pool, content_type='text/plain')

