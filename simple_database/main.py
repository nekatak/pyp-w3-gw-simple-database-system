import os
import json
from .config import BASE_DB_FILE_PATH
from datetime import date
from .exceptions import ValidationError

def create_database(db_name):
    return Make_db(db_name)

def connect_database(db_name):
    raise NotImplementedError()


class Make_db(object):
    def __init__(self, db_name):
        self.db_name=db_name
        if os.path.exists(BASE_DB_FILE_PATH + db_name):
            message='Database with name '+'"'+db_name+'"' +' already exists.'
            raise ValidationError(message)
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


class Table(object):
    def __init__(self,db_name, name, columns):
        self.db_name=db_name
        self.table_name=name
        self.columns={}
        self.rows=0
        fout=open(BASE_DB_FILE_PATH + db_name + "/" + name, 'wt')
        self.columns['structure']=columns
        data = json.dumps(self.columns)
        fout.write(data)
        fout.close()



    def insert(self,*args):

        if len(self.columns["structure"])<len(args):
            raise ValidationError("Invalid amount of fields.")
        fout = open(BASE_DB_FILE_PATH + self.db_name + "/" + self.table_name, 'at')
        ndict={}
        i=0
        for item in self.columns["structure"]:
            name = item["name"]
            if type(args[i]) is not eval(item["type"]):
                raise ValidationError('Invalid type of field %s: Given %s"str", expected %s"date"')%(name, type(args[i]), item["type"])
            val = args[i]
            if type(args[i]) is date:
                val=str(args[i])
            ndict[name]=val
            i+=1
        data = json.dumps(ndict)
        self.rows+=1
        fout.write(data)
        fout.close()


    def count(self):
        return self.rows


    def describe(self):
        return self.columns["structure"]
