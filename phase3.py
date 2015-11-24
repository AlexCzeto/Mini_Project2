import sys
from bsddb3 import db 
from datetime import *
import datetime


rwfile = 'rw.idx'
rtfile = 'rt.idx'
ptfile = 'pt.idx'
scfile = 'sc.idx' 

rwDB = db.DB()
rtDB = db.DB()
ptDB = db.DB()
scDB = db.DB()


def search_similar (start_b,last_b,rec_id,ind):
	"""
	Searches particial terms on given index 
	Input : start result, last result, result set, indx
	Return: result set
	"""
	cur_start= ind.cursor()
	cur_end= ind.cursor()
	result = cur_start.set_range(start_b)
	last = cur_end.set_range(last_b)

	while(result!=last):
		rec_id.add(result[1])
		result = cur_start.next()
	return rec_id

def search_p(key):
	"""
	Search for product title terms that exactly match user string
	Input: user entered string
	Return: set of record id's
	"""
	rec_ids = set()

	# check for %
	if key.find("%") != -1:
	 	key = key.strip("%")
	 	end = key[:-1]+ chr(ord(key[-1])+1)

	 	key = bytes(key,'ascii')
	 	end = bytes(end,'ascii')
	 	rec_ids = search_similar(key,end,rec_ids,ptDB)
	else:
		key = bytes(key, 'ascii')
		cur = ptDB.cursor()
		
		# find first key,data pair with matching key and 
		# set the cursor to that location
		result = cur.set(key)
		
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
				amount = amount-1
	return rec_ids

def search_r(key):
	"""
	Search for review terms that exactlty match string
	Input: user entered string
	Return: set of record id's
	"""
	rec_ids = set()

	if key.find("%") != -1:
		key = key.strip("%")
		end = key[:-1]+ chr(ord(key[-1])+1)

		key = bytes(key,'ascii')
		end = bytes(end,'ascii')
		rec_ids = search_similar(key,end,rec_ids,rtDB)
	else:
		key = bytes(key, 'ascii')
		cur = rtDB.cursor()
		result = cur.set(key)
		if (result != None):
			amount = cur.count()-1
			rec_ids.add(result[1])
			while(amount > 0):
				result = cur.next()
				rec_ids.add(result[1])
				amount = amount-1
	return rec_ids

def search_rscore(operator, value):
	"""
	Given an operator (< or >) and a value, returns a list of 
	records whose review score is either smaller than or greater than
	the given value
	"""
	if operator == ">":
		#print("searching rscore for values greater than " + str(value))
		result = sc_greater(value)
	if operator == "<":
		#print("searching rscore for values smaller than " + str(value))
		result = sc_less(value)
	return result	
	
def sc_less(score):
	"""
	Finds all record ids that have a lower score then "score"
	Uses set_range to finds lowest index with the same or 
	barely greater value then the given score.
	Move back one to find first lower score.
	Continue moving backwards until you get "NULL" also know 
	as the beginning the end of the index
	"""
	rec_id = set()
	score = bytes(score,'ascii')
	cur = scDB.cursor()

	result = cur.set_range(score)
	result = cur.prev()

	if result != None:
		while(result != None):
			rec_id.add(result[1])
			result = cur.prev()
	return(rec_id)


def sc_greater(score):
	"""
	Finds all record ids that have a greater score then "score"
	Uses set_range to finds lowest index with the same or 
	barely greater value then the given score.
	Move back one to find first lower score.
	Continue moving backwards until you get "NULL" also know 
	as the beginning the end of the index
	"""
	rec_id = set()
	score = bytes(score,'ascii')
	cur = scDB.cursor()

	# Check that there is any result bigger or equal
	result = cur.set_range(score)

	if(result == None):
		return rec_id

	# If score equals result, we most find the position
	while(result[0]==score):
		result = cur.next()
		if (result == None):
			return rec_id

	while (result != None):
		rec_id.add(result[1])
		result = cur.next()

	return rec_id

def search_pprice(operator, price, records):
	"""
	Given a list of records, a price and an operator (> or <)
	returns a list of records from the given list whose price 
	price either greater than or smaller than the given price
	"""
	if operator == ">":
		#print("searching pprice for values greater than " + str(price))
		results = set()

		for record_id in records:
			# Retrieve the record from the database
			record = rwDB.get(record_id)
			record = record.decode("utf-8")
		
			# Isolate the price
			record = record.split('"')
			record = record[2].split(",")
			try:		 
				r_price = float(record[1])
			except ValueError:
				r_price = -1

			# Remove the record if it does not satisfy the given condition
			if r_price > price:
				# remove record with record_id from the set records
				results.add(record_id)
		return results

	if operator == "<":
		results = set()

		for record_id in records:
			# Retrieve the record from the database
			record = rwDB.get(record_id)
			record = record.decode("utf-8")
		
			# Isolate the price
			record = record.split('"')
			record = record[2].split(",")
			try:		 
				r_price = float(record[1])
			except ValueError:
				r_price = -1

			# Remove the record if it does not satisfy the given condition
			if (r_price < price) & r_price != -1:
				# remove record with record_id from the set records
				results.add(record_id)
		return results

def search_rdate(operator, value, records):
	"""
	Given a list of records, a date and an operator (> or <)
	returns a list of records from the given list whose date 
	are either greater than or smaller than the date
	"""
	value = value.split("/")
	date = datetime.datetime(int(value[0]), int(value[1]), int(value[2]))
	results = set()

	if operator == ">":
		#print("searching rdate for values greater than " + str(value))
		for record_id in records:
			# Retrieve the record from the database
			record = rwDB.get(record_id)
			record = record.decode("utf-8")

			# Isolate the timestamp
			timestamp = record.split('"')
			timestamp = timestamp[4].split(",")
			timestamp = timestamp[3]
			record_date = date.fromtimestamp(int(timestamp))

			# Compare, remove if necessary
			if record_date > date:
				results.add(record_id)
		return results

	if operator == "<":
		#print("searching rdate for values smaller than " + str(value))
		for record_id in records:
			# Retrieve the record from the database
			record = rwDB.get(record_id)
			record = record.decode("utf-8")

			# Isolate the timestamp
			timestamp = record.split('"')
			timestamp = timestamp[4].split(",")
			timestamp = timestamp[3]
			record_date = date.fromtimestamp(int(timestamp))

			# Compare, remove if necessary
			if record_date < date:
				results.add(record_id)
		return results
	
def search_query(query, rec_ids):
	"""
	Given a single query, returns a list of records which 
	match the query
	"""	
	if "pprice" in query:
		operator = query[6]
		value = int(query[7:])
		results = search_pprice(operator, value, rec_ids)

	elif "rscore" in query:
		operator = query[6]
		value = query[7:]
		results = search_rscore(operator, value)

	elif "rdate" in query:
		operator = query[5]
		value = query[6:]
		results = search_rdate(operator, value, rec_ids)

	# SEARCHING FOR TERMS:
	elif query.find(":") != -1: 
		query = query.split(":")
		if query[0] == 'p':
			results = search_p(query[1])
		elif query[0] == 'r':
			results = search_r(query[1])
	else:
		results = search_r(query)
		results = results.union(search_p(query))

	return results


def merge_range_query(terms, index):
	"""
	given a list of search terms, terms, and 
	an index, i, determines if all elements needed
	for the range query are in the same or consecutive 
	elements in the list, and combines them if necessary 
	
	returns: terms, a list with element i properly formatted, 
	with the same number of elements as the list passed to it
	( adds " " to the end of the list for every join )
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
	# queries converted to lowercase
	query = query.lower()
	
	# must convert dates to timestamps

	# Split the query on spaces, then remove them
	query_list = query.split(" ")
	query_list = [x for x in query_list if x != ""]

	# look for pprice, rscore, and rdate,string and combine
	# them into one element if necessary:
	for i in range(len(query_list)-1):
		if query_list[i].find("pprice") != -1:
			merge_range_query(query_list, i)
		if query_list[i].find("rscore") != -1:
			merge_range_query(query_list, i)
		if query_list[i].find("rdate") != -1:
			merge_range_query(query_list, i)

	# Get rid of the extra spaces generated in the merge functions
	query_list = [x for x in query_list if x != " "]

	# Reorder the list so that
	# searches on pprice, rscore, and rdate do not appear first
	for i in range(len(query_list)):
		query = query_list[i]
		if ("pprice" not in query) & ("rscore" not in query) & ("rdate" not in query) :
 			# switch this term with term 0 in the list
 			query_list[0], query_list[i] = query_list[i], query_list[0]

 	# rec_ids will be our set of results
	search_results = set()	
	
	# set the first search result to the empty set
	search_results = search_query(query_list[0], search_results)
	query_list.pop(0)

	# for every following query, intersect the old search and new search
	for query in query_list:
		search_results = search_results.intersection(search_query(query, search_results))
		
	return(search_results)

def print_results(search_results):
	"""
	Takes a set of records and prints them for display

	Each query returns the full record of the matching review,
	with review id given first, followed by the rest of the fields 
	formatted for output display, in some readable format. 

	each review record consists of a product id, a product title, 
	a product price, userid and profile name of the reviewer, helpfulness
	of the review, review score, review timestamp, summary and full text 
	of the review,
	"""
	for result in search_results:
		result = rwDB.get(result)
		text = result.decode("utf-8")
		text = text.split('"')
		time = int(text[4].split(",")[3])
		time = date.fromtimestamp(time)

		# The only thing is the &quot which we have to change back
		# into actual quotes, for the title, name, summary, and text fields
		review_title =  text[1].replace('&quot;', '"')
		profile_name = text[3].replace('&quot;', '"')
		summary =  text[5].replace('&quot;', '"')
		review_text = text[7].replace('&quot;', '"')

		print("review id: " + text[0].strip(","))
		print("review title: " + review_title)
		print("Price: " + text[2].split(",")[1])
		print("Userid: " + text[2].split(",")[2] )
		print("Profile Name of reviewer: " + profile_name)
		print("Review Helpfulness: " +text[4].split(",")[1])
		print("Review Score: " + text[4].split(",")[2])
		print("Review Timestamp: " + str(time))
		print("Review Summary: " + summary)
		print("Review Text: " + review_text)
		print("\n")


def main():
	# Open seperate databases for each index
	rwDB.open(rwfile)
	rtDB.open(rtfile)
	ptDB.open(ptfile)
	scDB.open(scfile)
	search = input("Enter your search query: ")
	results = process_query(search)
	print_results(results)
	print(len(results))

if __name__ == "__main__":
	main()
