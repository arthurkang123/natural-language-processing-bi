import textrazor
import numpy


def convertToTR(to_analyze):
	textrazor.api_key = "3aabf0ff52c0a10edf94a5d8bc713d9d5564ee4c32e752a39eca0e99"
	# textrazor.api_key = "55b2da9fa0fe9274aaa55e302687c0348a430f40ecf19255be505f06"

	client = textrazor.TextRazor(extractors=["entities", "words", "phrases","dependency-trees",\
		"relations", "entailments","spelling"])
	response = client.analyze(to_analyze)
	return response

def getNounPhraseHead(noun_phrase, to_analyze):
    word_list = numpy.empty(len(noun_phrase.words), dtype=object)
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
    # head of noun phrase should always be the head of the first element
    return curr_word

"""
return an dictionary mapping head with the noun phrases
"""
def getNounPhrases(to_analyze,column_names):
    """
    possibly useful things
    - spelling_suggestions : word property
    """
    dictionaryHeadToPhrase = {}
    response = convertToTR(to_analyze)
    validPhrase = False
    for noun_phrase in response.noun_phrases():
        for word in noun_phrase.words:
            # check whether phrase is relevant
            word_to_analyze = to_analyze[word.input_start_offset:word.input_end_offset]
            for column_name in column_names:
                if word_to_analyze in column_name:
                    validPhrase = True
                    break
        if validPhrase:
            dictionaryHeadToPhrase[getNounPhraseHead(noun_phrase, to_analyze)] = noun_phrase
            validPhrase = False

    return dictionaryHeadToPhrase
"""
def convertTextRazorToEng(dict):
	newdict = {}

	for head in dict:
		temp = []
		for x in dict[head].words:
			temp.append(to_analyze[x.input_start_offset: \
						x.input_end_offset])
						newdict[head] = temp"""
#need list of operations that will be supported
	#these will be used to match against in our phrase,
#autocomplete

def parseS(phrase, column_names):
    colToVal = {}
    outputStringFieldColName = ""
    # outputStringFieldValue = []
    mapping = getNounPhrases(phrase,column_names)
    prevWord = ''
    outputStringDescriptors = ''

    for head in mapping:
        currIndex = 0
        if head != "year":
            outputStringDescriptors += head
        for word in mapping[head].words:
            # print(to_analyze[word.input_start_offset: \
            # word.input_end_offset])
            # case when we are looking at a year
            englishWordList = numpy.empty(len(mapping[head].words), dtype=object)

            if word.part_of_speech == 'CD':
                colToVal['Year'] = phrase[word.input_start_offset: \
                                              word.input_end_offset]
            # made the assumption that prevword would be the field value
            if word.part_of_speech == "NN" \
                    or word.part_of_speech == "NNS" \
                    or word.part_of_speech == "JJ" \
                    or word.part_of_speech == "NNP":
                englishWordList[currIndex] = phrase[word.input_start_offset: \
                                                        word.input_end_offset]
                curr_word = englishWordList[currIndex]
                if curr_word not in column_names:
                    prevWord = curr_word
                if curr_word in column_names and curr_word != head:
                    outputStringFieldColName = curr_word
                    colToVal[outputStringFieldColName] = prevWord
                    # outputStringFieldValue.append(englishWordList[currIndex-1])
                    prevWord = curr_word
    return outputStringDescriptors, colToVal
"""
def isInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False
       
        """

"""

def simpleParse(to_analyze):
	response = convertToTR(to_analyze)
	for noun_phrase in response.noun_phrases():
		curr_noun_phrase = to_analyze[noun_phrase.words[0].input_start_offset: \
				noun_phrase.words[-1].input_end_offset]
		dictionary[getNounPhraseCol(noun_phrase)] = curr_noun_phrase
	return dictionary
	
def getNounPhraseCol(noun_phrase):

	for word in noun_phrase.words:
		curr_word = to_analyze[word.parent.input_start_offset: \
			word.parent.input_end_offset]
		if curr_word in column_names:
			descriptorToValDict["cu"]
			word_list.append(curr_word)
		word_list = []

		if(word.part_of_speech == NN or word.part_of_speech == CD):
			word_list.append(curr_word)

	#head of noun phrase should always be the head of the first element
	return word_list[0]

#ex. employee count of each gender, for the staff in the department of Economics in the year 2017
descriptorToValDict = {}
#what is  the economics department headcount for the year 2018?

"""
#What was the average revenue in 2018?
#print(parseS(to_analyze,["headcount","department","year"]))
#print(parseS(to_analyze,["revenue","year"]))
if __name__ == "__main__":
    to_analyze = "what is the economics department count for the year 2017"
    column_names = ["headcount","department","year"]
    print(parseS(to_analyze,column_names))
