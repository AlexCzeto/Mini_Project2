from bsddb3 import db 

rwfile = 'rw.idx'
rtfile = 'rt.idx'
ptfile = 'pt.idx'
#scfile = 'sc.idx' 

rwDB = db.DB()
rtDB = db.DB()
ptDB = db.DB()
scDB = db.DB()




def search_similar_p(term):
	print("searching p for similar to " + term)

def search_similar_r(term):
	print("searching r for similar to " + term)

def search_p(key):
	"""
	Search for product title terms that exactly match user string
	Input: user entered string
	Return: set of record id's
	"""
	# check for %
	# Which we're not doing for now
	if key.find("%") != -1:
	 	key = key.strip("%")
	 	search_similar_p(key)
	else:
		key = bytes(key, 'ascii')
		rec_ids = set()
		cur = ptDB.cursor()
		# find first key,data pair with matching key and 
		#set the cursor to that location
		result = cur.set(key)
		#print(result)
		if (result != None):
		# The cursor will move forward in relation to 
		# how many keys matched the string
		# This will retirate over all matching key,data pairs that match.
			amount = cur.count()-1
			# Add first id to set and then contuine iterating 
			rec_ids.add(result[1])
			while(amount > 0):
				result = cur.next()
				# Data set contains only byte literals 
				rec_ids.add(result[1])
				print(result)
				amount = amount-1

			print(rec_ids)
		else:
			print("Search returned no result")
		return rec_ids





def search_r(key):
	"""
	Search for review terms that exactlty match string
	Input: user entered string
	Return: set of record id's
	"""
	# LETS THINK ABOUT THIS LATER OKAY
	if key.find("%") != -1:
		key = key.strip("%")
		search_similar_r(key)
	else:
		key = bytes(key, 'ascii')
		rec_ids = set()
		cur = rtDB.cursor()
		result = cur.set(key)
		print(result)
		if (result != None):
			amount = cur.count()-1
			rec_ids.add(result[1])
			while(amount > 0):
				result = cur.next()
				rec_ids.add(result[1])
				print(result)
				amount = amount-1
			print(rec_ids)
		else:
			print("Search returned no result")
		return rec_ids


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
			
			r_results = set(search_r(query))
			p_results = set(search_p(query))
			results.append(r_results.union(p_results))

	# We're just going to have to figure out how to join all these now
	print(results)

def main():
	# Open seperate databases for each index
	rwDB.open(rwfile)
	rtDB.open(rtfile)
	ptDB.open(ptfile)
#	scDB.open(scfile)
	string = input('Enter word to search : ')
	process_query(string)

	# print("---------------------------------------------------------------------------------------------")
	# print("\nResults of search by review text\n")
	# rec_ids1 = search_r(byte)
	# rec_ids = set(rec_ids1)
	

	# #find_results(rec_ids1)
	# print("---------------------------------------------------------------------------------------------")
	# print("\nResults of search by title\n")
	# rec_ids2 = search_p(byte)
	# rec_ids = rec_ids.union(rec_ids2)
	# #find_results(rec_ids2)

	# print("---------------------------------------------------------------------------------------------")
	# print("\nResult of OR title and review terms")
	# print(rec_ids)


if __name__ == "__main__":
	main()

