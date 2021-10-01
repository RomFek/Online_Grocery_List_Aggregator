from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient import errors
import base64
import email as em
from bs4 import BeautifulSoup
import re
import operator
# We only need read only access
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
#NOTE: If you experience an issue with disabled API for your project, then follow the instructions on the following page to create a new project.
#replace the credentials file in your working directory and delete the token file before running the code:
#https://developers.google.com/gmail/api/quickstart/php
class email_reader():
    
    def __init__(self):    
        print("[INFO] Email reader initialized.")
        
    #Get an authorized Gmail API service instance.
    def GetService(self):
    
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        #NOTE: If getting an error when refreshing a token, please delete token.pickle from the working directory and run the code again to generate new access.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)
    
        service = build('gmail', 'v1', credentials=creds)
        return service
    
    
    #GET A LIST OF IDS OF MESSAGES THAT MATCH THE QUERY
    def ListMessagesMatchingQuery(self, service, user_id='me', query='from:hemkop@kund.hemkop.se AND subject:*kvitto*'):
        print('[INFO] Retrieving the list of messages...')
        try:
            
            response = service.users().messages().list(userId=user_id,
                                                   q=query).execute()
            
            messages = []
            
            if 'messages' in response:
                messages.extend(response['messages'])
            while 'nextPageToken' in response:
                page_token = response['nextPageToken']
                response = service.users().messages().list(userId=user_id, q=query,
                                                 pageToken=page_token).execute()
                messages.extend(response['messages'])
        except errors.HttpError as error:
            print ('[ERROR] An error occurred while retrieving the list of messages: ')
            print(error)
        else:
            return messages
    
    #GET A MESSAGE DETAILS
    def GetMessage(self, service, user_id, msg_id):
        try:
            message = service.users().messages().get(userId=user_id, id=msg_id).execute()
            #print ('Message snippet: ' + message['snippet'])
        except errors.HttpError as error:
            print ('[ERROR] An error occurred: ' + error)
        else:
            return message
            
    def GetMimeMessage(self, service, user_id, msg_id):
        try:
            message = service.users().messages().get(userId=user_id, id=msg_id,
                                                     format='raw').execute()
            msg_str = base64.urlsafe_b64decode(message['raw'].encode('ASCII'))
            #print(msg_str)
            mime_msg = em.message_from_string(msg_str.decode('utf-8'))
        except errors.HttpError as error:
            print ('[ERROR] An error occurred: ' + error)
        else:
            return mime_msg
    
    def ExtractReceiptsFromEmails(self):
        service = self.GetService()
        emails = self.ListMessagesMatchingQuery(service)
        print('[INFO] Processing {count} emails...'.format(count = len(emails)))
        try:
            receipts = [] 
            for email in emails:
                receipt_data = {}#Intialize a dictionary to hold the email date and receipt data
                message_id = email['id']
                #Extracting the email receival date
                email_date_part = self.GetMessage(service, 'me', message_id)
                #Extract the portion containing the date
                email_date_raw = email_date_part['payload']['headers'][1]['value']
                #Clean up the extracted date payload, by outlining the actual date segment
                email_date = re.findall(r'\w{3},\s\d{1,2}\s\w{3}\s\d{4}', email_date_raw)
                #Extracting the part containing the receipt
                content = str(self.GetMimeMessage(service, 'me', message_id))
                #From the raw message, extract only the html portion, by finding the index of the html tags
                html_start_index = content.find('<html>') 
                html_end_index = content.find('</html>')
                html_content = content[html_start_index:html_end_index + len('</html>')]
                #Process the extracted html using BeautifulSoap
                soup = BeautifulSoup(html_content, features="html5lib")
                receipt = soup.findAll('pre')
                receipt_str = str(receipt)    
                #Find consecutive repeating hyphens in the receipt. The items tend to be placed between the first pair of hyphens
                found_consecutive_typhens = re.findall(r'((\-)\2{2,})', str(receipt))
                first_hyphen_series_start_index = receipt_str.find(found_consecutive_typhens[0][0])
                #Find the length of the consecutively repeated hyphens so that we can exclude all of it from the string
                consecutive_hyphens_length = len(found_consecutive_typhens[0][0])
                #Find the start of the second series of consecutively repeated hyphens by searching for them after their first occurrence in the string
                second_hyphen_series_start_index = receipt_str.find(found_consecutive_typhens[0][0], first_hyphen_series_start_index + consecutive_hyphens_length)
                cleaned_receipt = receipt_str[first_hyphen_series_start_index + consecutive_hyphens_length + 1:second_hyphen_series_start_index]
            
                receipt_data['date'] = email_date[0]
                receipt_data['receipt'] = cleaned_receipt
                receipts.append(receipt_data)
        except errors.HttpError as error:
            print("[ERROR] Failed to extract receipts from emails.")
            print(error)
        else:
            return receipts
        
    def extractRecieptItems(self):
        receipts = self.ExtractReceiptsFromEmails()
        cleaned_receipts = []
        if receipts != None:
            counter = 0
            try:
                for receipt in receipts:
                    clean_receipt = {}
                    cleaned_items = []
                    receipt_body = receipt['receipt'] 
                    lines = receipt_body.splitlines() #Split the string receipt body into lines
                    #Process the receipt body line by line
                    item_counter = 0
                    for item in lines:
                        if operator.not_(re.match(r'\s', item)) and operator.not_(re.match(r'Extrapris', item)) : #Exclude lines with the consecutive spaces, because those usually contain discount info rather than the product info
                            item_details = {}
                            price = re.findall(r'(\d{1,5},\d{1,2})', item) #Extract price from the receipt line
                            #Sometimes each item has additional info about cost (e.g. discount, recycle etc.)
                            if len(price) > 0:
                                price_chosen = price[len(price) - 1]
                            else:
                                price_chosen = 0
                            item_counter += 1
                            item_details['item_number'] = item_counter
                            item_details['price'] = price_chosen
                            item_details['item_name'] = item[:item.find('  ')] #Extract product name from the receipt line. Product name comes before consecutive spaces (at least 3)
                            cleaned_items.append(item_details)
                    counter += 1
                    clean_receipt['receipt_id'] = counter
                    clean_receipt['receipt_date'] = receipt['date']
                    clean_receipt['receipt_items_count'] = len(cleaned_items)     
                    clean_receipt['receipt_items'] = cleaned_items     
                    cleaned_receipts.append(clean_receipt)   
                    #print("------------------------------")
            except errors.HttpError as error:
                print("[ERROR] An error occurred when processing one of the receipts")
                print(error)
                return None
            else:
                return cleaned_receipts
        else:
            print("[ERROR] An error occurred in the previous step when reading email from email server.")
            
#sc = email_reader()
#for r in sc.extractRecieptItems():
        #print(r)             
#if __name__ == '__main__':  
    #for r in extractRecieptItems():
        #print(r)       