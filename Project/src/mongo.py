import pymongo
from pymongo import MongoClient
import pandas as pd
import json
path_folder_csv = "C:/Users/BRAYAN LIPE/Documents/UNSA/2020/SEMESTRE B/Proyecto Final de Carrera/Project/files/dataset.csv"

path_folder_csv_out = "C:/Users/BRAYAN LIPE/Documents/UNSA/2020/SEMESTRE B/Proyecto Final de " \
                  "Carrera/Project/files/outfile.csv "


class MongoDB(object):

    def __init__(self, dBName=None, collectionName=None):

        self.dBName = dBName
        self.collectionName = collectionName

        self.client = MongoClient("localhost", 27017, maxPoolSize=50)

        self.DB = self.client[self.dBName]
        self.collection = self.DB[self.collectionName]

    def insertData(self, path):

        df = pd.read_csv(path, engine='python')
        print(df.info())
        data = df.to_dict('records')

        self.collection.insert_many(data, ordered=False)
        print("All the Data has been Exported to Mongo DB Server .... ")

    def queries(self, dBName=None):
        print("help")
        mydb = self.client[self.dBName]
        mycol = mydb['Programas']
        my_queries = {"DEPARTAMENTO_FILIAL": "LIMA"}
        mydoc = mycol.find(my_queries)
        for x in mydoc:
            print(x)

if __name__ == "__main__":
    mongodb = MongoDB(dBName='MongoDB', collectionName='Programas')
    #mongodb.insertData(path_folder_csv_out)
    mongodb.queries(dBName='MongoDB')