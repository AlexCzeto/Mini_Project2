class Review:
	"""
	Because why the fuck not?

	Now without the tags inside of the attributes!
	Or trailing newlines!
	"""
	def __init__(self, attributes):
		self.pid = attributes[0].split(' ', 1)[1].rstrip('\n')
		self.ptitle = attributes[1].split(' ', 1)[1].rstrip('\n')
		self.price = attributes[2].split(' ', 1)[1].rstrip('\n')
		self.userid = attributes[3].split(' ', 1)[1].rstrip('\n')
		self.username = attributes[4].split(' ', 1)[1].rstrip('\n')
		self.helpful = attributes[5].split(' ', 1)[1].rstrip('\n')
		self.rscore = attributes[6].split(' ', 1)[1].rstrip('\n')
		self.rtime = attributes[7].split(' ', 1)[1].rstrip('\n')
		self.summary = attributes[8].split(' ', 1)[1].rstrip('\n')
		self.ftext = attributes[9].split(' ', 1)[1].rstrip('\n')


def phase_one():
	"""
	 Read the file "small-date.txt" and write it into 
	 the four files described in the assignment spec
	 """

	#filename = input("Enter the name of the file: ")
	# or no!

	txt = open("small-data.txt")
	
	# Create a new file, replacing " with &quot
	# and \ with \\
	temp = open("temp.txt", 'w')
	temp.write(txt.read().replace('"', '&quot').replace("\\", "\\\\"))
	temp.close()
	temp = open("temp.txt", 'r')
	
	# create the review objects
	reviews_list = []
	single_r = []
	for line in temp.readlines():
		if line == '\n':
			reviews_list.append(Review(single_r))
			single_r = []
		else:
			single_r.append(line)
		
	# create "reviews.txt"	
	reviews = open("reviews.txt", 'w')
	j = 0
	for i in reviews_list:
		j = j + 1
		reviews.write(str(j)+","+i.pid +',"'+i.ptitle+'",'+i.price+","+i.userid+',"'+i.username+'",'+i.helpful+","+i.rscore+","+i.rtime+',"'+i.summary+'","'+i.ftext+'"\n')
	reviews.close()

	#pterms.txt: This file includes terms of length 3 or more characters 
	#extracted from product titles; a term is a consecutive sequence of 
	#alphanumeric and underscore '_' characters, i.e [0-9a-zA-Z_] or the 
	#character class \w in Perl or Python. The format of the file is as 
	#follows: for every term T in a product title of a review with id I, 
	#there is a row in this file of the form T',I where T' is the lowercase 
	#form of T. That means, terms must be converted to all lowercase before
	#writing them in this file. Here is the respective file for our sample 
	#file with 10 records.
	pterms = open("pterms.txt", 'w')
	j = 0
	for i in reviews_list:
		# for every product title:
		j =  j + 1
		product_title = i.ptitle
		# find every pterm:
		product_title = product_title.split(" ")
		for words in product_title:
			# take out non-alphanumeric characters
			# re.sub(r'\W+', '', your_string)
			words = words.lower()
			words = re.sub(r'\W+', ' ', words).split(" ")
			for word in words:
			# if it's long enough, write it to the file
				if len(word) > 2:
					# and write them to the file
					pterms.write(word+","+str(j)+"\n")
		
	pterms.close()


	# rterms.txt: This file includes terms of length 3 or more characters 
	# extracted from the fields review summary and review text. The file 
	# format and the way a term is defined is the as given above for the
	# file pterms.txt. 
	rterms = open("rterms.txt", 'w')
	j = 0
	for i in reviews_list:
		# for every product title:
		j =  j + 1
		summary = i.summary
		ftext = i.ftext
		# find every pterm:
		summary = summary.split(" ")
		ftext = ftext.split(" ")
		for words in summary:
			# take out non-alphanumeric characters
			words = words.lower()
			words = re.sub(r'\W+', ' ', words).split(" ")
			for word in words:
			# if it's long enough, write it to the file
				if len(word) > 2:
					# and write them to the file
					rterms.write(word+","+str(j)+"\n")
		
		for words in ftext:
			# take out non-alphanumeric characters
			words = words.lower()
			words = re.sub(r'\W+', ' ', words).split(" ")
			for word in words:
			# if it's long enough, write it to the file
				if len(word) > 2:
					# and write them to the file
					rterms.write(word+","+str(j)+"\n")

	rterms.close()


	# scores.txt: This file includes one line for each review record in the form 
	# of sc:I where sc is the review score and I is the review id. 
	scores = open("scores.txt", 'w')
	j = 0
	for i in reviews_list:
		j = j + 1
		scores.write(str(j)+":"+i.rscore+'\n')

	scores.close()

	# close our files
	txt.close()
	temp.close()

phase_one()





def search_similar_p(term):
	print("searching p for similar to " + term)

def search_similar_r(term):
	print("searching r for similar to " + term)

def search_p(term):
	# check for %
	if term.find("%") != -1:
		term = term.strip("%")
		search_similar_p(term)
	else:
		print("Searching p for " + term)

def search_r(term):
	if term.find("%") != -1:
		term = term.strip("%")
		search_similar_r(term)
	else:
		print("Searching r for " + term)

def search_pprice(operator, value):
	if operator == ">":
		print("searching pprice for values greater than " + str(value))
	if operator == "<":
		print("searching pprice for values smaller than " + str(value))

def search_rdate(operator, value):
	if operator == ">":
		print("searching rdate for values greater than " + str(value))
	if operator == "<":
		print("searching rdate for values smaller than " + str(value))

def search_rscore(operator, value):
	if operator == ">":
		print("searching rscore for values greater than " + str(value))
	if operator == "<":
		print("searching rscore for values smaller than " + str(value))


def merge_range_query(terms, index):
	"""
	given a list of search terms, terms, and 
	an index, i, determines if all elements needed
	for the range query are in the same or consecutive 
	elements in the list, and combines them if necessary 
	
	returns: terms, a list with element i properly formatted, 
	with the same number of elements as the list passed to it

	To do this, adds " " to the end of the list for every join
	"""
	# Merge > or < with the previous element (if necessary)
	if terms[index].find(">") == -1 & terms[index].find("<") == -1:
		terms[index:index+2] = [''.join(terms[index:index+2])]
		terms.append(" ")

	# Add the next element if necessary
	# If < or > is the last character in terms[i], then we have to merge
	# the next term with it 
	if terms[index].find(">") == (len(terms[index])-1):
		terms[index:index+2] = [''.join(terms[index:index+2])]
		terms.append(" ")

	if terms[index].find("<") == (len(terms[index])-1):
		terms[index:index+2] = [''.join(terms[index:index+2])]
		terms.append(" ")

	return terms




def process_query(query):
	query = query.lower()
	
	# Split the query on spaces, then remove them
	query_list = query.split(" ")
	query_list = [x for x in query_list if x != ""]
	try:
	# look for pprice, rscore, and rdate,string and combine
	# them into one element if necessary:
		for i in range(len(query_list)-1):
			if query_list[i].find("pprice") != -1:
				merge_range_query(query_list, i)
			if query_list[i].find("rscore") != -1:
				merge_range_query(query_list, i)
			if query_list[i].find("rdate") != -1:
				merge_range_query(query_list, i)
	except IndexError as e:
		print(e)

	# Get rid of the extra spaces
	query_list = [x for x in query_list if x != " "]


	# So, now that we have all of everything formatted uniformly,
	# We can do all our searches
	results = []	# This will contain the search results
	for query in query_list:

		# SEARCHING FOR A RANGE:
		if "pprice" in query:
			operator = query[6]
			value = int(query[7:])
			results.append(search_pprice(operator, value))
	
		elif "rscore" in query:
			operator = query[6]
			value = int(query[7:])
			results.append(search_rscore(operator, value))
	
		elif "rdate" in query:
			operator = query[5]
			value = query[6:]
			results.append(search_rdate(operator, value))
		

		# SEARCHING FOR TERMS:
		elif query.find(":") != -1: 
			query = query.split(":")
			if query[0] == 'p':
				results.append(search_p(query[1]))
			elif query[0] == 'r':
				results.append(search_r(query[1]))
		else:
			results.append(search_p(query))
			results.append(search_r(query))

	# We're just going to have to figure out how to join all these now
	#print(results)


# You can use this code to test out process_query
#string = input("Enter your query thing: ")
#string = "r:a  p:test pprice > 788 p:terms rscore < 123  rdate > 233 camera"
#string = "pprice > 055"
#process_query(string)





