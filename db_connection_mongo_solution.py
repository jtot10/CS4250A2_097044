

#-------------------------------------------------------------------------
# AUTHOR: Joshua Totten 
# FILENAME: CS4250A2_097044
# SPECIFICATION: PyMongo database 
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here

from pymongo import MongoClient
from collections import defaultdict
import datetime

def connectDataBase():
    
    # Create a database connection object using pymongo
    # --> add your Python code here

    client = MongoClient("mongodb://localhost:27017/")
    db = client["document_database"]
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # create a dictionary to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    
    term_count = defaultdict(int)
    terms = docText.lower().split()
    for term in terms:
        term_count[term] += 1
        
    # create a list of dictionaries to include term objects. [{"term", count, num_char}]
    # --> add your Python code here        
        
    term_objects = [{"term": term, "count": count, "num_char": len(term)} for term, count in term_count.items()]

    #Producing a final document as a dictionary including all the required document fields
    # --> add your Python code here
    
    document = {
    "docId": docId,
    "docText": docText,
    "docTitle": docTitle,
    "docDate": docDate,
    "docCat": docCat,
    "terms": term_objects
    }

    # Insert the document
    # --> add your Python code here
    col.insert_one(document)
    
def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here

    col.delete_one({"docId": docId})
    
def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here

    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
        
    index = defaultdict(lambda: defaultdict(int))
    documents = col.find()

    for doc in documents:
        for term in doc["terms"]:
            term_text = term["term"]
            count = term["count"]
            title = doc["docTitle"]
            index[term_text][title] += count
            
    formatted_index = {term: ",".join(f"{title}:{count}" for title, count in title.items()) for term, title in index.items()}
    return formatted_index

    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here