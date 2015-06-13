'''
Created on Jun 6, 2014

@author: Cassie
'''
import starbase
import os
import sys
import email
from starbase import Connection
    
c = Connection(port=8080)

#Create table
t = c.table('table1')
t.create('content')


#Read data from files
address = '/home/public/course/enron_mail_20110402/maildir'
idnumber = 0


for directory in os.listdir(address):
    idnumber += 1
    path = os.path.join(address, directory).replace("\\","/")
    path_sent = os.path.join(path, 'sent').replace("\\","/")
    if os.path.isdir(path_sent):
        for filename in os.listdir(path_sent):
            file_path = os.path.join(path_sent, filename).replace("\\","/")
            with open(file_path, "r") as myfile:
                data=myfile.read()
                msg = email.message_from_string(data)
                sender = msg['From']
                date = msg['Date']
                msgid = msg['Message-ID']
                date_split = date.split()
                month = date_split[2]
                content = msg.get_payload()
                rowkey = `idnumber`+'-'+sender+'-'+msgid
                #Insert data
                t.insert(rowkey,{
                                 'content':{'key11':content}
                                 })
'''
t.insert(
     'my-key-1',
     {
         'content': {'key11': 'value 11', 'key12': 'value 12', 'key13': 'value 13'}
    }
 )
'''