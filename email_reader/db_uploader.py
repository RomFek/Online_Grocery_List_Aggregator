import sqlite3
from email_reader import email_reader 
from apiclient import errors
from datetime import datetime
from decimal import Decimal

#A class used for creating and incrementaly updating the database with the receipt data retrieved from Gmail account. 
class db_uploader():
    
    def __init__(self):    
        print("[INFO] DB Uploader initialized.")
        self._conn, self._cur, self._table_count = self.initialize_db()
                 
    @property 
    def connection(self):
        return self._conn
        
    @property 
    def cursor(self):
        return self._cur
         
    @property 
    def table_count(self):
        return self._table_count
       
    #Initialize the DB connection
    def initialize_db(self):
        try:
            print("[INFO] Initializing the database...")
            conn = sqlite3.connect("groceries.db")
            cursor = conn.cursor()
        except errors.HttpError as error:
            print("[ERROR] An error occurred when initializing the database.")
            print(error)
        else:
            try:
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('receipts', 'items');")
                results = cursor.fetchall()
            except errors.HttpError as error3:
                print(error3)
                print("[ERROR] An error occurred while checking existense of 'items' and 'receipts' tables.")
            else:
                table_count = len(results) #If "receipts" and "items" tables already exist then the count will be 2
                return conn, cursor, table_count
    
    #Delete "items" and "receipts" tables in case if there is a need to purge all data. 
    def delete_tables(self):
        if self.table_count == 0:
            print("[INFO] The tables 'receipts' and 'items' do  not exist in the DB.")
        else:
            print("[INFO] Deleting the tables...")
            try:
                self.cursor.execute("DROP TABLE items;")
                self.cursor.execute("DROP TABLE receipts;")
                self.connection.commit()
                self.connection.close()
                print("[INFO] Deleted tables successfully.")
            except errors.HttpError as error:
                print("[ERROR] Failed to delete tables.")
                print(error)
    
    #Pass cursor as input parameter
    def create_tables(self, cursor):
        #Check if receipts and items tables already exist
        if self.table_count == 0:
            print("[INFO] Creating 'receipts' and 'items' tables...")
            try:
                cursor.execute("CREATE TABLE receipts (receipt_id int, receipt_date text, item_count int, created_date text, PRIMARY KEY (receipt_id));")
            except errors.HttpError as error1:
                print("[ERROR] Failed to create 'receipts' table")
                print(error1)
            try:
                cursor.execute("CREATE TABLE items (item_id int, item_name text, item_price DECIMAL(7, 3), receipt_id int, PRIMARY KEY (item_id), FOREIGN KEY (receipt_id) REFERENCES receipts(receipt_id));")
                self.connection.commit()
            except errors.HttpError as error2:
                print("[ERROR] Failed to create 'items' table")   
                print(error2)
        else:
            print("[INFO] Tables 'receipts' and 'items' already exist. New data will be appended.")
    
    #Pass email reader as a input parameter
    def populate_tables(self, reader, close_post_update = False):
        self.create_tables(self.cursor)
        #Get a list of receipt IDs that are already in the DB
        try:
            self.cursor.execute("SELECT r.receipt_id from receipts r group by r.receipt_id;")
            receipts_in_db_raw = self.cursor.fetchall()
            receipts_in_db = []
            for d in receipts_in_db_raw:
                receipts_in_db.append(d[0])
                #print(d[0])
        except:
            print("[ERROR] An error occurred while retrieving a list of existing records.")
        else:
            receipts_data = reader.extractRecieptItems()
            print("[INFO] Populating tables...")
            try:
                for r in receipts_data:
                    receipt_date = r['receipt_date']
                    receipt_date_formated = datetime.date(datetime.strptime(receipt_date, '%a, %d %b %Y')) #Since SQLite does not have Date data type, we need to pre-process date using datetime Python functions
                    receipt_id = "{}{}{}{}".format(r['receipt_id'], receipt_date_formated.day, receipt_date_formated.month, receipt_date_formated.year)
                    curr_timestamp = datetime.today().replace(microsecond=0) #Get the current timestamp (without milliseconds)
                    #Add receipt to DB if a record with the same receipt unique ID is not already in the DB
                    if int(receipt_id) not in receipts_in_db:
                        print("printing: " + str(receipt_id))
                        receipt_items = r['receipt_items']
                        receipt_items_count = r['receipt_items_count']
                        #Add receipt record to receipts table
                        insert_command = "INSERT INTO receipts VALUES({id}, '{recep_date}', {count}, '{created_date}');".format(id = receipt_id, recep_date = receipt_date_formated, count = receipt_items_count, created_date = curr_timestamp)
                        self.cursor.execute(insert_command)
                        #print("-------------------")
                        try:
                            for product in receipt_items:
                                #print(product)
                                product_id = "{}{}".format(product['item_number'], receipt_id)
                                product_name = product['item_name'].replace(',','.') #Replace commas with a dot so that it accidentely it will not mess up the insert statement
                                product_price = product['price']
                                product_price_in_decimal = Decimal(str(product_price).replace(',','.'))
                                receipt_id_fk = receipt_id
                                #Add receipt item to items table
                                insert_item_command = "INSERT INTO items VALUES({id}, '{name}', {price}, {recep_id});".format(id = product_id, name = product_name, price = product_price_in_decimal, recep_id = receipt_id_fk)
                
                                #print(insert_command)
                                self.cursor.execute(insert_item_command)
                                #As we are using SQLite, which does not have Double data type, we need to use Integer data type.
                                #You want to store $1.01. This is the same as 101 cents. You just multiply by 100.
                                #So before storing the number, multiply by 100. When displaying the number, divide by 100.
                        except errors.HttpError as error1:
                            print("[ERROR] An error occurred while populating 'items' table.")
                            print(error1)
                    self.connection.commit()
            except errors.HttpError as error2:
                print("[ERROR] An error occurred while populating tables.")
                print(error2)
            else:
                print("[INFO] Tables populated successfully.")
                if close_post_update:
                    self.connection.close()    
            
    #Get data from DB. Provide a valid SELECT statement to read the data. 
    def get_data(self, query):
        invalid_keywords = ["DELETE ", "INSERT ", "UPDATE ", "ALTER ", "CREATE "]
        if any(k in query.upper() for k in invalid_keywords):
            print("[ERROR] Found invalid keyword in the provided query.")
        else:
            print("[INFO] Query validated. No invalid SQL commands found. Proceeding to retrieve data from the DB...")
            try:
                self.cursor.execute(query)
                results = self.cursor.fetchall()
            except errors.HttpError as error:
                print("[ERROR] An error occurred while reading data from the DB")
                print(error)
            else:
                return results
        
#du = db_uploader()
#du.delete_tables()
du = db_uploader()
data = du.get_data("select * from receipts;")
reader = email_reader()
du.populate_tables(reader)

#data = du.get_data("SELECT r.receipt_id, r.receipt_date, r.item_count, r.created_date, i.* from receipts r join items i on i.receipt_id = r.receipt_id")
#data = du.get_data("SELECT r.receipt_id from receipts r group by r.receipt_id")

#for d in data:
#    print(d)
#print(my_list)
#dateTimeObj = datetime.today().replace(microsecond=0)
#print(dateTimeObj)