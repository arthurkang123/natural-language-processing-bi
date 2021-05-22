# Imports the Google Cloud client library
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
import re
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "bi project-0f579c1da4be.json"

#(pos_tag[token.part_of_speech.tag]!='PRON' and pos_tag[token.part_of_speech.tag]!='VERB' and pos_tag[token.part_of_speech.tag]!='DET' and pos_tag[token.part_of_speech.tag]!='PUNCT')
def clean_data(text, column_names):
    #remove punctuation
    if(text[len(text)-1] == '.' or text[len(text)-1] == '?' ):
        text = text[:len(text)-1]
    #remove unnecessary spacing
    text = re.sub(' +',' ',text)
    for index in range(len(column_names)):

        #convert all column names to lower case
        column_names[index] = column_names[index].lower()


        for word in text.split(' '):
            if column_names[index] in word or word in column_names[index]:
                column_names[index] = word
    return text,column_names
def syntax_text(text,columns):

    text,columns = clean_data(text,columns)
    print('Text: {}'.format(text))
    # Instantiates a client
    client = language.LanguageServiceClient()

    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    """Detects syntax in the text."""
    client = language.LanguageServiceClient()

    # Instantiates a plain text document.
    document = types.Document(
        content=text,
        type=enums.Document.Type.PLAIN_TEXT)

    # Detects syntax in the document. You can also analyze HTML with:
    #   document.type == enums.Document.Type.HTML
    tokens = client.analyze_syntax(document).tokens

    """don't analyze word if it is a preposition(ex.a), determinant(ex.the), or conjunction(ex.and)"""
    def notExtraneousWord(label):
        return label!='DET' and label!='PREP' and  label!='CC'

    #(token.dependency_edge.head_token_index == 1 or token.dependency_edge.head_token_index==0) and \
            #(pos_tag[token.part_of_speech.tag]!='PRON' and pos_tag[token.part_of_speech.tag]!='VERB' and \
                #pos_tag[token.part_of_speech.tag]!='DET' and pos_tag[token.part_of_speech.tag]!='PUNCT'))
    # part-of-speech tags from enums.PartOfSpseech.Tag
    pos_tag = ('UNKNOWN', 'ADJ', 'ADP', 'ADV', 'CONJ', 'DET', 'NOUN', 'NUM','PRON', 'PRT', 'PUNCT', 'VERB', 'X', 'AFFIX')
    outputStringValue = [] #this will hold the main noun, ex. headcount, salary
    colToDescriptor={} #this will hold the mapping of the entry in columns(ex. department) to its descriptors(ex. math, econ)
    lengthOfSentence = len(tokens)
    currColName = ""
    headPos = 0
    adj = None
    action = None
    insert = True
    for token in tokens:

        if( len(str(token.dependency_edge).split()) ==2):
            label = str(token.dependency_edge).split()[1]
        else:
            label = str(token.dependency_edge).split()[3]
            headPos = int(str(token.dependency_edge).split()[1])
        word = token.text.content
        headWordToken = tokens[headPos]
        print(word)
        wordInC = False
        #if label is the subject of the noun phrase then this is the val

        for c in columns:
            if headWordToken.text.content in c:
                wordInC = True
        if( len(str(headWordToken.dependency_edge).split()) ==2):
            headwordlabel = str(headWordToken.dependency_edge).split()[1]
        else:
            headwordlabel = str(headWordToken.dependency_edge).split()[3]
            headPos = int(str(headWordToken.dependency_edge).split()[1])

        headWordString = headWordToken.text.content
        if label == "AMOD" and (headwordlabel == "NSUBJ" or headwordlabel == "DOBJ" and headWordString in columns) or \
            (label == "AMOD" and (headwordlabel == "ROOT" and headWordString in columns)) or \
            (label == "AMOD" and (headwordlabel == "CONJ" and headWordString in columns)):
            action = word
            print("adj" + action)
            continue
        if(label == "NSUBJ" or label == "DOBJ" and word in columns):
            print("value: " + word)
            outputStringValue.append([word,action])
        #case for when we have an incomplete sentence
        elif label == "ROOT" and word in columns:
            print("value: " + word)
            outputStringValue.append([word,action])
        #case for when we have a second value for same entry in columns
        elif(label == "CONJ" and word in columns):
            outputStringValue.append([word,action])
        #check that this word is a descriptor if it's head is a column in table
        elif (headWordString in columns or wordInC) and notExtraneousWord(label) and word not in columns and label!= "CONJ":
            currColName = headWordString
            colToDescriptor[headWordString] = [word]
            print("descriptor: " + word)
        #case for when we have a second descriptor attached to same col value
        elif (label == "CONJ" or label == "NUM") and currColName in columns and notExtraneousWord(label) and label!='CC':
            colToDescriptor[currColName].append(word)
            print("descriptor: " + word)
        elif(word in columns and headwordlabel == "POBJ"):
            colToDescriptor[word] = [headWordString]
<<<<<<< HEAD
        #ARTHUR
        #elif 
        #ARTHUR END
        
=======

>>>>>>> 5743d396ce60af85a98fbe06171bfdec64bb4895
        print("label: " + label)
        print("headPos:" + str(headPos))
        print("headWord:"+ headWordString)
        print()
    print("output")
    print(outputStringValue)
    print(colToDescriptor)
    #Ensures good format for value counts on backend
    x = outputStringValue[0][0]
    if x in colToDescriptor.keys():
        val = colToDescriptor.pop(x)
        if None in outputStringValue[0]:
            outputStringValue[0].remove(None)
        outputStringValue[0].append(val[0])
        print(outputStringValue)
        print(colToDescriptor)

    print(outputStringValue,colToDescriptor)
    return outputStringValue,colToDescriptor

#expected:(['head count'], {department:[economics, math],year:{2018}})
#What is the economics department headcount and salary for the year 2018?
#["headcount","salary","department","year"]
#
print()
print(syntax_text('What is the assault in arizona state',["state","assault"]))

from pythonFlaskApp import *
<<<<<<< HEAD
for key in key_value_dict:
        print(key, key_value_dict[key])
get_dict()
print("TESTING")



=======
#get_dict()
>>>>>>> 5743d396ce60af85a98fbe06171bfdec64bb4895
