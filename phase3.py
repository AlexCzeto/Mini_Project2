from bsddb3 import db 

rwfile = 'rw.idx'
rtfile = 'rt.idx'
ptfile = 'pt.idx'
scfile = 'sc.idx' 

rwDB = db.DB()
rtDB = db.DB()
ptDB = db.DB()
scDB = db.DB()

# Search for product title terms that exactly match user string
# Input: user entered string
# Return: set of record id's
def search_prodt(key):
	rec_id = set()
	cur = ptDB.cursor()
# find first key,data pair with matching key and set the cursor to that location
	result = cur.set(key)
	#print(result)
	if (result != None):
# The cursor will move forward in relation to how many keys matched the string
# This will retirate over all matching key,data pairs that match.
		amount = cur.count()-1
# Add first id to set and then contuine iterating 
		rec_id.add(result[1])
		while(amount > 0):
			result = cur.next()
# Data set contains only byte literals 
			rec_id.add(result[1])
			print(result)
			amount = amount-1

		print(rec_id)
	else:
		print("Search returned no result")
	return rec_id

# Search for review terms that exactlty match string
# Input: user entered string
# Return: set of record id's
def search_revt(key):
	rec_id = set()
	cur = rtDB.cursor()
	result = cur.set(key)
	print(result)
	if (result != None):
		amount = cur.count()-1
		rec_id.add(result[1])
		while(amount > 0):
			result = cur.next()
			rec_id.add(result[1])
			print(result)
			amount = amount-1
		print(rec_id)
	else:
		print("Search returned no result")
	return rec_id

# MAYBE USE DB MULTI??????!!!???!?!??!
def search_part(key):
	rec_id = set()
	end = key[:-1]+ chr(ord(key[-1])+1)

	key = bytes(key,'ascii')
	end = bytes(end,'ascii')

	rec_id.union(search_p_di(key,end,rec_id,ptDB))
	rec_id.union(search_p_di(key,end,rec_id,rtDB))

	print(rec_id)
	return(rec_id)

def search_p_di(start_b,last_b,rec_id,ind):
	cur_start= ind.cursor()
	cur_end= ind.cursor()
	result = cur_start.set_range(start_b)
	print("start : ",result)
	last = cur_end.set_range(last_b)

	while(result!=last):
		rec_id.add(result[1])
		result = cur_start.next()

	return rec_id

		
"""
	for i in range(0,4):
		result = cur_start.next()
		if(result == last):
			break
		print(result)
"""
# Finds all record ids that have a lower score then "score"
# Uses set_range to finds lowest index with the same or barely greater 
# value then the given score.
# Move back one to find first lower score.
# Continue moving backwards until you get "NULL" also know as the beginning the end of the index
def sc_less(score):
	rec_id = set()
	score = bytes(score,'ascii')
	cur = scDB.cursor()

	result = cur.set_range(score)
	#print("original result :",result)

	result = cur.prev()

	if (result == None):
		print("No result")
	else:
		while(result != None):
			rec_id.add(result)
			result = cur.prev()
	print(rec_id)
	return(rec_id)

def sc_greater(score):
	rec_id = set()
	score = bytes(score,'ascii')
	cur = scDB.cursor()

	# Check that there is any result bigger or equal

	result = cur.set_range(score)
#	print(result)

	if(result == None):
		print("No greater result")
		return rec_id
	# If score equals result, we most find the position where 
	while(result[0]==score):
#		print(result)
		result = cur.next()
		if (result == None):
			print("No greater result")
			return rec_id

	while (result != None):
		print(result)
		rec_id.add(result[1])
		result = cur.next()
	print(rec_id)
	return rec_id

def search_price(rec_id):
	if(rec_id):
		print(rec_id)
		while(rec_id):
			key = rec_id.pop()
			result = rwDB.get(key)
			text = result.decode("utf-8")
			print(text)
			print("\n\n")

# Take set of record keys and prints them 		
def find_results(rec_id):
	if (rec_id):
		while(rec_id):
			key = rec_id.pop()
			result = rwDB.get(key)
			text = result.decode("utf-8")
			print(text)
			
			print("\n\n")
	else:
		print("No result")	

def main():

# Open seperate databases for each index
	rwDB.open(rwfile)
	rtDB.open(rtfile)
	ptDB.open(ptfile)
	scDB.open(scfile)
	string = input('Enter word to search : ')
	string = string.lower()
	rec_id=search_part(string)
	#byte = bytes(string, 'ascii')

	search_price(rec_id)

	#score = input("Enter score > :")
	#sc_greater(score)


	
	# print("---------------------------------------------------------------------------------------------")
	# print("\nResults of search by review text\n")
	# rec_ids1 = search_revt(byte)
	# rec_ids = set(rec_ids1)

	# find_results(rec_ids1)

	# print("---------------------------------------------------------------------------------------------")
	# print("\nResults of search by title\n")
	# rec_ids2 = search_prodt(byte)
	# rec_ids = rec_ids.union(rec_ids2)
	# find_results(rec_ids2)

	# print("---------------------------------------------------------------------------------------------")
	# print("\nResult of OR title and review terms")
	# print(rec_ids)


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
