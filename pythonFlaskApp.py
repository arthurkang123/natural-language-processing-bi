from logging.config import dictConfig
from flask import Flask, flash, redirect, render_template, request, session, abort, url_for
import os
import re
from pymongo import MongoClient
import json
from werkzeug.utils import secure_filename
import pandas as pd
import numpy as np
from gcpParser import syntax_text
from bson.code import Code
from collections import defaultdict
from pprint import pprint
import logging

client = MongoClient("localhost:27017")
dbString = 'CalAnswers'
db = client[dbString]
app = Flask(__name__)
UPLOAD_FOLDER = os.getcwd() + '/uploads/'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx', 'json'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

curr_cols = None
curr_cols_dict = dict()
cols_to_collection = dict()  # IMPORTANT: Column names are assumed to be unique

logging.basicConfig(filename='app.log',
                    filemode='a',
                    level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')


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
            global curr_file_name
            curr_file_name = filename
            import_content(filename)
            return redirect(url_for('index'))
    return render_template('index.html')


def list_to_projection_dict(project_list):
    """
    Format the the list of projection field to a dictionary
    in the format required by MongoDB.
    Ref: https://docs.mongodb.com/manual/reference/operator/projection/positional/
    """
    if project_list is None or project_list == []:
        return {}
    ones = [1 for _ in project_list]
    return dict(zip(project_list, ones))


def build_project_pipeline(raw_projection_list, accumulators):
    """
    Ref1: https://docs.mongodb.com/manual/reference/operator/aggregation/project/#pipe._S_project
    Ref2: https://docs.mongodb.com/manual/reference/operator/projection/positional/

    Construct the $project stage dictionary/document in the aggregation pipeline
    from the raw_projection_list returned from the NLP engine

    The aggregation operator/accumulator corresponding to the projection field
    is stored in the accumulators dictionary

    :param raw_projection_list: A nested list that contains list-formatted pairs
                                of field name and aggregation operator/accumulator
                                Note: operator can be None
                                [[fieldname1, operator1], [fieldname2, operator2], ...]
    :param accumulators: A dictionary that has projection fields as keys
                         and their operators as values.
                         It is modified in-place in this function
    :return: [see Ref2] A dictionary that contains {fieldname1: 1, fieldname2: 1, ...}
                        AND a list of the projection fields in lower case

    """
    global curr_cols
    projection_list = list()
    if raw_projection_list:
        for p, aggregator in raw_projection_list:
            new_p = None
            for col_name in curr_cols:
                if p.lower() in re.split(r'[\s_\-]+', col_name.lower()):
                    new_p = col_name
                    projection_list += [new_p]
                    accumulators[new_p] = aggregator
            if new_p is None:
                # If no matching column/field name, return None
                return None
    projection_dict = list_to_projection_dict(projection_list)
    if projection_dict:
        projection_dict['_id'] = 0
    print(accumulators)
    print(projection_list)
    return projection_dict, projection_list


def build_match_pipeline(raw_selection_qry):
    """
    Ref1: https://docs.mongodb.com/manual/reference/operator/aggregation/match/
    Ref2: https://docs.mongodb.com/manual/tutorial/query-documents/#read-operations-query-argument

    Construct the $match stage dictionary/document in the aggregation pipeline
    from the raw_selection_qry from the NLP engine
    :param raw_selection_qry: A dictionary that has field name as keys and
                              lists of field value(s) as the dictionary values
                            {`fieldname1`: [`value1`, `value2`, ...], `fieldname2`: [`value3`, ...], ...}
    :return: A dictionary that contains the query to be used in the $match stage
    """
    global curr_cols
    query = dict()
    for field, value_list in raw_selection_qry.items():
        new_key = None
        for col_name in curr_cols:
            if field.lower() in re.split(r'[\s_\-]+', col_name.lower()):
                new_key = col_name
        if new_key is None:
            return ''
        for i, value in enumerate(value_list):
            if value.isdecimal():
                value_list[i] = int(value)
            elif isfloat(value):
                value_list[i] = float(value)
        query[new_key] = {"$in": value_list}
    print(query)
    return query


def build_group_pipeline(projection_list, accumulators):
    """
    Ref1: https://docs.mongodb.com/manual/reference/operator/aggregation/group/#pipe._S_group

    Construct the $match stage dictionary/document in the aggregation pipeline
    based on the accumulators modified from `build_project_pipeline`

    :param projection_list: A list of all the projection fields in lower case
    :param accumulators: A dictionary that has projection fields as keys
                         and their operators as values.
    :return: A dictionary in this format
             { _id: <expression>, <field1>: { <accumulator1> : <expression1> }, ... }
    """
    group_dict = {'_id': None}
    if projection_list:
        # TODO: OR check `raw_projection_list` instead of `projection_list`
        for projection in projection_list:
            if not accumulators[projection] or accumulators[projection].lower() in ['total', 'sum']:
                group_dict["total_" + projection] = {'$sum': '$' + projection}
            elif accumulators[projection].lower() in ['mean', 'average']:
                group_dict["average_" + projection] = {'$avg': '$' + projection}
            elif accumulators[projection].lower() in ['least', 'lowest', 'minimum']:
                group_dict["minimum_" + projection] = {'$min': '$' + projection}
            elif accumulators[projection].lower() in ['greatest', 'most', 'maximum']:
                group_dict["maximum_" + projection] = {'$max': '$' + projection}
            elif accumulators[projection].lower() in ['value', 'values']:
                group_dict["distinct_values_of_" + projection] = {'$addToSet': '$' + projection}
            elif accumulators[projection].lower() in ['standard deviation', 'sd']:
                group_dict["sd_of_" + projection] = {'$stdDevPop': '$' + projection}

    print(group_dict)
    return group_dict


@app.route("/getRecords", methods=['POST'])
def getRecords():
    try:
        global curr_cols
        if curr_cols is None:
            return ""

        record_list = []
        pipeline = []
        accumulators = dict()

        user_input = str(request.data.decode("utf-8")).lower()
        if user_input == "":
            return ""

        app.logger.info("user_input: %s", user_input)

        # raw_projection_list: projection; raw_selection_qry: selection
        # raw_projection_list: [[`fieldname1`, aggregator1], [`fieldname2`, aggregator2], ...]
        # raw_selection_qry: {`fieldname1`: [`value1`, `value2`, ...], `fieldname2`: [...], ...}
        raw_projection_list, raw_selection_qry = syntax_text(user_input, list(curr_cols))
        app.logger.info("raw_projection_list: %s", raw_projection_list)
        app.logger.info("raw_selection_qry: %s", raw_selection_qry)

        # Construct the $project stage in the aggregation pipeline from the raw_projection_list
        projection_dict, projection_list = build_project_pipeline(raw_projection_list, accumulators)

        if not projection_dict:
            # If no matching column/field name, return an empty string
            return ''

        # Construct the $match stage in the aggregation pipeline from the raw_selection_qry
        query = build_match_pipeline(raw_selection_qry)

        app.logger.debug("query: %s", json.dumps(query))
        app.logger.debug("projection_dict: %s", json.dumps(projection_dict))

        pipeline.append({'$match': query})
        pipeline.append({'$project': projection_dict})

        # Construct the $group stage in the pipeline
        group_dict = build_group_pipeline(projection_list, accumulators)

        if len(group_dict) > 1:
            pipeline.append({'$group': group_dict})

        # Make MongoDB queries using aggregation pipeline
        # in the order of: selection, projection, grouping
        collection = cols_to_collection[projection_list[0]]
        print(collection)
        print(pipeline)
        records = db[collection].aggregate(pipeline)
        record_list = list(records)

        if len(group_dict) == 1:
            # If no accumulator is specified for any projection field
            # list all the values correspond to that projection field
            if raw_projection_list:
                result_dict = dict()
                for projection in raw_projection_list:
                    values_set = set(value for dic in record_list for key, value in dic.items())
                    result_dict[projection] = list(values_set)
                    record_list = list()
                    record_list.append(result_dict)

        for d in record_list:
            # remove `_id` field for displaying
            if '_id' in d:
                d.pop('_id')
        print("result: " + json.dumps(record_list))
        app.logger.info("result: %s", json.dumps(record_list))
        return json.dumps(record_list)

    except KeyError:
        app.logger.error(str(KeyError))
        return ''


def import_content(file_name):
    global curr_cols_dict
    # cdir = os.path.dirname(__file__)
    # file_res = os.path.join(cdir, filepath)
    # filePath = UPLOAD_FOLDER + curr_file_name
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
    if file_name.lower().endswith(".csv"):
        data = pd.read_csv(file_path)
    elif file_name.lower().endswith(".xls") or file_name.lower().endswith(".xlsx"):
        data = pd.read_excel(file_path)
    elif file_name.lower().endwidth(".json"):
        data = pd.read_json(file_path)
    else:
        app.logger.error("File type not supported")
        raise ValueError("File type not supported")

    data.columns = map(str.lower, data.columns)
    cols = [c.lower() for c in data.columns if c.lower()[:8] != "unnamed:" and c.lower() != ""]
    data = data[cols]
    cols_to_lower_case = [c for c in cols if not np.issubdtype(data[c].dtype, np.number)]
    data[cols_to_lower_case] = data[cols_to_lower_case].apply(lambda x: x.astype(str).str.lower())

    print("BEFORE", curr_cols_dict)
    if file_name in curr_cols_dict:
        cols_to_remove = curr_cols_dict[file_name]
        for col in cols_to_remove:
            curr_cols.remove(col)
        del curr_cols_dict[file_name]

    print("MID", curr_cols_dict)

    extract_column_names(file_name, data)
    data_json = json.loads(data.to_json(orient='records'))

    # Remove collection with the same name
    db[file_name].remove()

    # Insert the collection into the database.
    db[file_name].insert(data_json)
    app.logger.info("New collection added: %s", file_name)
    print(list(db[file_name].find()))


def extract_column_names(file_name, data):
    col_names = data.keys()
    lower_cols = set()
    for c in col_names:
        lower_cols.add(c.lower())
    global curr_cols
    if curr_cols is None:
        curr_cols = set(lower_cols)
    else:
        for name in col_names:
            curr_cols.add(name.lower())
    curr_cols_dict[file_name] = list(lower_cols)
    for col in lower_cols:
        cols_to_collection[col] = file_name
    print(curr_cols)


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

#Arthur get_values start
def get_values(db_name, collection, key):
    _client = MongoClient()
    _db = _client[db_name]
    return _db[collection].distinct(key)
#Arthur get_values end

# Credit: https://stackoverflow.com/questions/2298870/get-names-of-all-keys-in-the-collection
def get_keys(db_name, collection):
    _client = MongoClient()
    _db = _client[db_name]
    mp = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = _db[collection].map_reduce(mp, reduce, "dummy_key")
    return result.distinct('_id')

# match_result = db['Sample_data.csv'].aggregate([
    # {'$match': {'department': 'economics', 'gender': 'male'}},
    # {'$match': {'department': {'$in': ['economics']}}},
    # {'$project': {'count': 1}},
    # {'$group': {'_id': None, 'sum': {'$sum': '$count'}}}
# ])

# pprint(list(match_result))

def get_dict():
    for key in key_value_dict:
        print(key, key_value_dict[key])
    return key_value_dict

key_value_dict = dict()
# i.e. download column names and store in the program every time we load the program.
if __name__ == "__main__":
    for collection_name in db.collection_names():
        print(collection_name)
        if collection_name == "dummy_key":
            continue
        keys = get_keys('CalAnswers', collection_name)
        if "_id" in keys:
            keys.remove("_id")
        if "" in keys:
            keys.remove("")
        if curr_cols is None:
            curr_cols = set(keys)
        else:
            for k in keys:
                curr_cols.add(k.lower())

        curr_cols_dict[collection_name] = keys
        for k in keys:
            cols_to_collection[k] = collection_name
        print("keys: ", keys)
        for key in keys:
            key_value_dict[key] = get_values('CalAnswers', collection_name, key)
            print(key, key_value_dict[key])
        	#temp_val = get_values('CalAnswers', collection_name, key)
        	#print("values of", key, "are", temp_val)
    app.run()


# TODO: Produce finer grained results - make smaller multiple queries and post-process - refer to old code
