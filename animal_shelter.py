from pymongo import MongoClient
from bson.objectid import ObjectId 

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, USER, PASS, HOST, PORT, DB, COL):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        
        # Connection Variables - passed as arguments for flexibility
        USER = USER
        PASS = PASS 
        HOST = HOST 
        PORT = PORT
        DB = DB
        COL = COL
        
        # Initialize Connection
        try:
            self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
            self.database = self.client['%s' % (DB)]
            self.collection = self.database['%s' % (COL)]
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")
            # Optionally re-raise or handle more gracefully
            raise 

    def create(self, data: dict) -> bool:
        """
        Inserts a document into the MongoDB database and collection.
        Input: data - A dictionary representing the document to insert.
        Return: True if successful insert, else False.
        """
        if data:
            try:
                # Using insert_one for a single document
                result = self.collection.insert_one(data)
                # Check if the insert was acknowledged and an ID was generated
                if result.inserted_id:
                    print(f"Successfully inserted document with ID: {result.inserted_id}")
                    return True
                else:
                    print("Insert operation was not acknowledged.")
                    return False
            except Exception as e:
                print(f"An error occurred during create: {e}")
                return False
        else:
            raise Exception("Nothing to save, because data parameter is empty")
            return False

    def read(self, query: dict) -> list:
        """
        Queries for documents from the MongoDB database and collection.
        Input: query - A dictionary representing the key/value lookup pair.
        Return: A list of matching documents if successful, else an empty list.
        """
        if query is not None: # Allow empty query to fetch all, or specific query
            try:
                # Using find() which returns a cursor
                results_cursor = self.collection.find(query)
                results_list = list(results_cursor) # Convert cursor to list
                
                # Optionally, print how many documents were found
                return results_list
            except Exception as e:
                print(f"An error occurred during read: {e}")
                return [] # Return an empty list on error
        else:
            # If query is None, it's ambiguous. For safety, treat as error or return all.
            # Here, returning empty list for consistency with error handling.
            print("Error: Query parameter cannot be None for read operation.")
            return []
        
    def update(self, query: dict, update_data: dict) -> int:
        """
        Updates document(s) from a specified collection based on a query.
        Input: 
            query - The key/value lookup pair to find documents.
            update_data - A dictionary with the key/value pairs to update,
                        typically using MongoDB's $set operator.
        Return: The number of objects modified in the collection.
        """
        if query and update_data:
            try:
                result = self.collection.update_many(query, {"$set": update_data})
                modified_count = result.modified_count
                print(f"Successfully modified {modified_count} document(s).")
                return modified_count
            except Exception as e:
                print(f"An error occurred during update: {e}")
                return 0
        else:
            print("Error: Query and update_data parameters must be provided.")
            return 0

    def delete(self, query: dict) -> int:
        """
        Deletes document(s) from a specified collection based on a query.
        Input: query - The key/value lookup pair to find documents to delete.
        Return: The number of objects removed from the collection.
        """
        if query: # Safety check to ensure a query is provided.
            try:
                result = self.collection.delete_many(query)
                deleted_count = result.deleted_count
                print(f"Successfully deleted {deleted_count} document(s).")
                return deleted_count
            except Exception as e:
                print(f"An error occurred during delete: {e}")
                return 0
        else:
            print("Error: A non-empty query parameter is required to prevent accidental mass deletion.")
            return 0