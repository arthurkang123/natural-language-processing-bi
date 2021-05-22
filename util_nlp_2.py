import textrazor
import numpy
import re
import time


def convertToTR(to_analyze):
	textrazor.api_key = "55b2da9fa0fe9274aaa55e302687c0348a430f40ecf19255be505f06"

	client = textrazor.TextRazor(extractors=["entities", "words", "phrases","dependency-trees",\
		"relations", "entailments","spelling"])
	response = client.analyze(to_analyze)
	return response

def getNounPhraseHead(noun_phrase,to_analyze):
	word_list =numpy.empty(len(noun_phrase.words), dtype=object)
	currParent = ""
	if len(noun_phrase.words) == 1:
		word = noun_phrase.words[0]
		curr_word = to_analyze[word.parent.input_start_offset: \
		word.parent.input_end_offset]
		return curr_word
	for word in noun_phrase.words:
		curr_word = to_analyze[word.parent.input_start_offset: \
		word.parent.input_end_offset]
		currParent = word.parent
		break
	#head of noun phrase should always be the head of the first element
	return curr_word
"""
return an dictionary mapping head with the noun phrases
"""
def getNounPhrases(to_analyze,column_names,response):

	"""
	possibly useful things
	- spelling_suggestions : word property
	"""
	dictionaryHeadToPhrase = {}
	
	validPhrase = False
	for noun_phrase in response.noun_phrases():
		
		for word in noun_phrase.words:
			#check whether phrase is relevant(if it contains column name or not)
			if to_analyze[word.input_start_offset: \
			word.input_end_offset] in column_names:
				validPhrase = True
		if validPhrase:
			dictionaryHeadToPhrase[getNounPhraseHead(noun_phrase,to_analyze)] = noun_phrase

	return dictionaryHeadToPhrase

#need list of operations that will be supported
	#these will be used to match against in our phrase,
#autocomplete
def classifyWords(noun_phrases):
	verbs = []
	adjectives = []
	nouns = []
	for noun_phrase in noun_phrases:
		for word in noun_phrase.words:
			if word.part_of_speech == "JJ" or word.part_of_speech == "JJR" or word.part_of_speech == "JJS":
				adjectives.append(word)
			elif word.part_of_speech == "VB" or word.part_of_speech == "VBD" or \
				word.part_of_speech == "VBG" or word.part_of_speech == "VBN" or \
				word.part_of_speech == "VBP":
				verbs.append(word)
			elif word.part_of_speech == "NN" or word.part_of_speech == "NNS" or \
				word.part_of_speech == "NNP" or word.part_of_speech == "NNPS":
				nouns.append(word)
	return verbs,adjectives,nouns

def convert_column_names(phrase,column_names):
	phrase = re.sub(r'[^\w\s]','',phrase)
	#if phrase.contains('  '):
	phrase_words = phrase.split(' ')
	
	for word in phrase_words:
		index = 0
		for col in column_names:
			if word in col:
				column_names[index] = word
			index+=1
	return column_names

def parseS(phrase,column_names):
	start_time = time.time()
	colToVal = {}
	outputStringFieldColName = ""
	#outputStringFieldValue = []
	response = convertToTR(phrase)
	noun_phrases = response.noun_phrases()
	#mapping = getNounPhrases(phrase,column_names,response)
	#create a list containing all the verbs and a list containing all the adjectives
	classifyOut = classifyWords(noun_phrases)
	verbs = classifyOut[0]
	adjectives = classifyOut[1]
	nouns = classifyOut[2]
	prevWord = ''
	outputStringDescriptors= ''
	colunn_names = convert_column_names(phrase,column_names)

	for noun_phrase in noun_phrases:
		head = getNounPhraseHead(noun_phrase,phrase)
		#print('head' + head)
		validPhrase = False

		#check if phrase contains a column name
		for word in noun_phrase.words:
			if word in column_names:
				validPhrase = True
		for word in noun_phrase.words:
			index = 0
			convertedWord = phrase[word.input_start_offset: \
								word.input_end_offset]
			if convertedWord == "year":
				nextWord = phrase[noun_phrase.words[index+2].input_start_offset: \
								noun_phrase.words[index+2].input_end_offset]
				colToVal['Year'] = nextWord
			else:
				if convertedWord == head and word in nouns and convertedWord in column_names:
					outputStringDescriptors+=convertedWord
				
				if word in nouns and convertedWord in column_names and convertedWord!=head:
					#print(convertedWord)
					colToVal[convertedWord] = prevWord
				prevWord = convertedWord
			index+=1
	print("--- %s seconds ---" % (time.time() - start_time))
	return outputStringDescriptors, colToVal

#work for department of economics
#ex. employee count of each gender, for the staff in the department of Economics in the year 2017
#descriptorToValDict = {}
#more descriptors?
#what is  the economics department headcount for the year 2018?
#how to find word in partial query
#would a descriptor always be a numerical amount
#What was the average revenue in 2018?
#print(parseS(to_analyze,["headcount","department","year"]))
#print(parseS(to_analyze,["revenue","year"]))
#won't work if have more than 2 spaces

print(parseS("what is the economics department headcount for the year 2018?",["headcount","department_desc","year"]))

