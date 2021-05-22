from flask import Flask, flash, redirect, render_template, request, session, abort, url_for
import os
from pymongo import MongoClient
import json
from werkzeug import secure_filename
# import csv
# import subprocess
import pandas as pd
import numpy as np
from util_nlp_2 import parseS
from gcpParser import syntax_text
from bson.code import Code
from collections import defaultdict
import time


client = MongoClient("localhost:27017")
dbString = 'CalAnswers'
db = client[dbString]

app = Flask(__name__)
# UPLOAD_FOLDER = '/Users/ZeroNineSeven/research/bi_proj/answers/uploads/'
# PA = os.getcwd()
UPLOAD_FOLDER = os.getcwd() + '/uploads/'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx', 'json'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# currFileName = None
currCols = None
currColsDict = dict()
sum_set = set()
avg_set = set()
# global_map = dict()

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route("/", methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            global currFileName
            currFileName = filename
            # extractColumnNames(filename)
            import_content(filename)
            return redirect(url_for('index'))
    return render_template('index.html')


# Query:
# Format 1: [ field1, field2, ..., query ]
# Format 2: [ query ]
# Where is query is space-separated
@app.route("/getRecords", methods=['POST'])
def getRecords():
    try:
        global currCols
        if currCols is None:
            return ""

        recordsList = []

        userInput = str(request.data.decode("utf-8")).lower()
        if userInput == "":
            return ""
        print('userInput: ' + userInput)

        projection, raw_query = parseS(userInput, list(currCols))
        print("---------PROJECTION---------")
        print(projection)
        print("----------------------------")

        print('projection: ' + projection)
        print('raw_query: ' + json.dumps(raw_query))
        raw_projectionList = list()
        raw_projectionList += [projection]

        projectionList = []
        if projection is not None and projection != "":
            for p in raw_projectionList:
                newP = None
                for columnName in currCols:
                    if p.lower() in columnName.lower():
                        newP = columnName
                        projectionList += [newP]
                if newP is None:
                    return ''
        projectionDict = listToProjectionDict(projectionList)

        query = dict()
        for k, v in raw_query.items():
            newKey = None
            for columnName in currCols:
                if k.lower() in columnName.lower():
                    newKey = columnName
            if newKey is None:
                return ''
            if v.isdecimal():
                v = int(v)
            elif isfloat(v):
                v = float(v)
            newValue = {"$in": [v]}
            query[newKey] = newValue
        print('query: ' + json.dumps(query))
        print('projectionDict: ' + json.dumps(projectionDict))
        # query = {"department": {"$in": ["economics"]}, "calender_year": {"$in": [2017]}}

        # collection_name = None
        # global_key = json.dumps(tuple((query, projectionDict)))
        # if global_key in global_map:
        #     return global_map[global_key]

        for collectionName in db.collection_names():
            if projection is None:
                records = db[collectionName].find(query)
            else:
                records = db[collectionName].find(query, projectionDict)
            if records.count() > 0:
                # collection_name = collectionName
                break

        for record in records:
            recordItem = record
            recordItem.pop("_id")
            recordsList.append(recordItem)

        agg_recordsList = defaultdict(list)
        resultDict = dict()
        if projection is not None and projection != "" and recordsList != []:
            if (projection in sum_set or projection in avg_set):
                for r in recordsList:
                    for key, value in r.items():
                        agg_recordsList[key].append(value)

                for key, li in agg_recordsList.items():
                    if key in sum_set:
                        resultDict[key] = sum(li)
                    elif key in avg_set:
                        resultDict[key] = sum(li) / len(li)
            else:
                valueSet = set(value for dic in recordsList for key, value in dic.items())
                resultDict[projection] = list(valueSet)
            # recordsList = agg_recordsList
            recordsList = []
            recordsList.append(resultDict)

        print("result: " + json.dumps(recordsList))
        # global_map[global_key] = json.dumps(recordsList)
        return json.dumps(recordsList)
    except KeyError:
        return ''


def listToProjectionDict(projectList):
    if projectList is None or projectList == []:
        return None
    ones = [1 for _ in projectList]
    return dict(zip(projectList, ones))


def import_content(fileName):
    # cdir = os.path.dirname(__file__)
    # file_res = os.path.join(cdir, filepath)
    # filePath = UPLOAD_FOLDER + currFileName
    filePath = os.path.join(app.config['UPLOAD_FOLDER'], fileName)
    if fileName.lower().endswith(".csv"):
        data = pd.read_csv(filePath)
    elif fileName.lower().endswith(".xls") or fileName.lower().endswith(".xlsx"):
        data = pd.read_excel(filePath)
    elif fileName.lower().endwidth(".json"):
        data = pd.read_json(filePath)
    else:
        raise ValueError("File type not supported")

    data.columns = map(str.lower, data.columns)
    cols = [c.lower() for c in data.columns if c.lower()[:8] != "unnamed:" and c.lower() != ""]
    data = data[cols]
    cols_to_lower_case = [c for c in cols if not np.issubdtype(data[c].dtype, np.number)]
    data[cols_to_lower_case] = data[cols_to_lower_case].apply(lambda x: x.astype(str).str.lower())

    extractColumnNames(fileName, data)
    data_json = json.loads(data.to_json(orient='records'))

    # db[fileName].insert(data_json)
    # data_dict = data.to_dict("records")
    # print(data_dict)
    db[fileName].remove()
    # db[fileName].insert(data_dict)
    # db[fileName].insert_many(data_dict)
    db[fileName].insert(data_json)
    print(list(db[fileName].find()))


def extractColumnNames(fileName, data):
    columnNames = data.keys()
    lowerCols = set()
    for c in columnNames:
        lowerCols.add(c.lower())
    global currCols
    if currCols is None:
        currCols = set(lowerCols)
    else:
        for name in columnNames:
            currCols.add(name.lower())
    currColsDict[fileName] = list(lowerCols)
    print(currCols)


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

# Credit: https://stackoverflow.com/questions/2298870/get-names-of-all-keys-in-the-collection
def get_keys(db_name, collection):
    client = MongoClient()
    db = client[db_name]
    map = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = db[collection].map_reduce(map, reduce, "dummy_key")
    return result.distinct('_id')


# i.e. download column names and store in the program every time we load the program.
if __name__ == "__main__":
    sum_set.add('count')
    avg_set.add('avg_age')
    avg_set.add('calender_year')
    for collectionName in db.collection_names():
        print(collectionName)
        if collectionName == "dummy_key":
            continue
        keys = get_keys('CalAnswers', collectionName)
        if "_id" in keys:
            keys.remove("_id")
        if "" in keys:
            keys.remove("")
        if currCols is None:
            currCols = set(keys)
        else:
            for k in keys:
                currCols.add(k.lower())

        currColsDict[collectionName] = keys
    app.run()

