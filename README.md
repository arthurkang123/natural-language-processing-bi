# UC Berkeley Business Intelligence Project 
Berkeley's current applications are not the best when it comes to building ad-hoc queries to answer simple high-level questions like "What is current employee head count of economics department". The campus would love to have a BI tool where ad-hoc queries can be built in an easier and simple way to answers some of the critical questions.

We want to build an interactive ad-hoc BI Reporting solution where users can get high-level data with a simple search query against the data source (Oracle Database, Flat File). The main aim of this application will be to simplify the way users build ad-hoc queries using key words (similar to twitter hashtags) such Year, Month, Department, Employee Count, Average Years of Service, Enrollment, Revenue, Budget etc. Application search/query page we want to display some example keywords and canned Queries.  The system should be able to identify the key words and if a not an exact match system should be able to recommend the best possible match.

## Overview
This is a simple web app that queries a MongoDB database using natural language built on Python + Flask, MongoDB, AngularJS, and HTML/CSS.

In a separate bash process, start a MongoDB instance by running:
```mongod```

Edits will need to be made on variable values because they are currently for use on my files and directory structure.

The index.html of this web app contains a search bar and an upload button. A user can upload a .csv file using the upload button, that will then be queried
by whatever is inside the search bar. This is accomplished by addCollection() in the app, which creates a subprocess that calls
mongoimport with the file, and imports the csv file into the database. Search queries are currently being processed as simple natural language statement/question. 
An example query would be:

```What is the economics department count for the female gender in the year 2016 and 2017?```

which would return the total headcount as numbers.

## Development Notes and Resourcese
The main technologies used in this project are:
* MongoDB - accessed using pymongo API
* Flask Framework
* AngularJS
* HTML/CSS

##### MongoDB
* Intro to MongoDB: 
    * https://www.youtube.com/watch?v=EE8ZTQxa0AM
    * https://www.youtube.com/watch?v=CvIr-2lMLsk
* MongoDB documentation: 
    * https://docs.mongodb.com/manual/
* Aggregation Pipeline: 
    * https://docs.mongodb.com/manual/aggregation/
    * https://docs.mongodb.com/manual/core/aggregation-pipeline/

##### Flask Framework
* Flask Documention: http://flask.pocoo.org/

##### AngularJS
* ...

##### 
* https://www.w3schools.com/html/html_css.asp



