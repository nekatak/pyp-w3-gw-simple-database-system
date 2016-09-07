import os
import json
from .config import BASE_DB_FILE_PATH
from datetime import date

def create_database(db_name):
    return Make_db(db_name)

def connect_database(db_name):
    raise NotImplementedError()


class Make_db(object):
    def __init__(self, db_name):
        self.db_name=db_name
        if not os.path.exists(BASE_DB_FILE_PATH + db_name):
            os.makedirs(BASE_DB_FILE_PATH + db_name)

    def show_tables(self):
        arr=[]
        path=BASE_DB_FILE_PATH+self.db_name
        for file in os.listdir(path):
            if file.endswith(".json"):
                arr.append(file)

        return arr

    def create_table(self, name, columns):
        #arrOfTables = self.show_tables
        #if name in arrOfTables:
            #raise ValidationError("Database with name %s already exists.")%name
        table = Table(self.db_name, name, columns)
        setattr(self, name, table)
        #return table

class Table(object):
    def __init__(self,db_name, name, columns):
        self.db_name=db_name
        self.table_name=name
        self.columns={}
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
            name=item["name"]
            if type( args[i]) is not eval(item["type"]):
                raise ValidationError('Invalid type of field %s: Given %s"str", expected %s"date"')%(name, type(args[i]), item["type"])
            ndict[name]=args[i]
            i+=1
        data = json.dumps(ndict)
        fout.write(data)
        fout.close()

    def count(self):
        with open(BASE_DB_FILE_PATH + db_name + "/" + table_name, 'rt'):
            data = json.load


    def describe(self):
        return self.columns["structure"]
