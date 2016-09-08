import os
import json
from .config import BASE_DB_FILE_PATH
from datetime import (date, datetime)
from .exceptions import ValidationError
import re
from collections import OrderedDict

def create_database(db_name):
    if os.path.exists(BASE_DB_FILE_PATH + db_name):
        message='Database with name '+'"'+db_name+'"' +' already exists.'
        raise ValidationError(message)
    return Make_db(db_name)

def connect_database(db_name):
    new_db=Make_db(db_name)
    path = BASE_DB_FILE_PATH + db_name
    for file in os.listdir(path):
        #print(file)
        fout=open(path+"/"+file, "rt")
        data_of_file=fout.read()
        data=json.loads(data_of_file, object_pairs_hook=OrderedDict)
        new_table=new_db.create_table(file, data['structure'])
        for elem in data["rows"]:
            new_values=[]
            #print(list(data["rows"]))
            for x in elem.values():
                #print x
                if type(x) is not int and type(x) is not bool and re.match('\d+-\d+-\d+', x):
                    x=datetime.strptime(x,'%Y-%m-%d').date()
                    new_values.append(x)
                else:
                    if type(x) is int or type(x) is bool:
                        new_values.append(x)
                    else:
                        new_values.append(str(x))
            new_table.insert( *new_values)
        fout.close()
    return new_db


class Make_db(object):
    def __init__(self, db_name):
        self.db_name=db_name

        if not os.path.exists(BASE_DB_FILE_PATH + db_name):
            os.makedirs(BASE_DB_FILE_PATH + db_name)

    def show_tables(self):

        arr=[]
        path=BASE_DB_FILE_PATH+self.db_name
        for file in os.listdir(path):
            arr.append(file)
        return arr


    def create_table(self, name, columns):
        if type(name) != str:
            raise TypeError('Invalid table name type.')
        table = Table(self.db_name, name, columns)
        setattr(self, name, table)
        return table


class Table(object):
    def __init__(self,db_name, name, columns):
        self.db_name=db_name
        self.table_name=name
        self.columns={}
        self.rows=0
        fout=open(BASE_DB_FILE_PATH + db_name + "/" + name, 'wt')
        self.columns['structure']=columns
        self.columns["rows"]=[]
        data = json.dumps(self.columns)
        fout.write(data)
        fout.close()



    def insert(self,*args):

        if not len(self.columns["structure"])==len(args):
            raise ValidationError("Invalid amount of fields.")
        fout = open(BASE_DB_FILE_PATH + self.db_name + "/" + self.table_name, 'wt')
        ndict=OrderedDict()
        i=0
        for item in self.columns["structure"]:
            name = item["name"]
            if type(args[i]) is not eval(item["type"]):
                message = 'Invalid type of field "%s": Given "%s", expected "%s"'%(name, type(args[i]).__name__, item["type"])
                raise ValidationError(message)
            val = args[i]
            if type(args[i]) is date:
                val=str(args[i])
            ndict[name]=val
            i+=1
        self.columns["rows"].append(ndict)
        data = json.dumps(self.columns)
        self.rows+=1
        fout.write(data)
        fout.close()


    def count(self):
        return self.rows


    def describe(self):
        return self.columns["structure"]

    def query(self, **kwargs):
        print (self.columns['rows'])
        #print (self.columns['rows'][1]['id'])
        arr=[]
        for x in self.columns["rows"]:
            if kwargs.keys()[0] in x:
                if x[list(kwargs.keys())[0]]==list(kwargs.values())[0]:
                    arr.append(TableObject(x))
        return arr

    def all(self):
        i=0
        while True:
            if i<len(self.columns["rows"]):
                yield TableObject(self.columns["rows"][i])##olo lathos prepei na gyrna TableObject
                i+=1
            else:
                raise StopIteration()


class TableObject():
    def __init__(self, ordDict ):
        for x in ordDict:
            setattr(self, x , ordDict[x] )
