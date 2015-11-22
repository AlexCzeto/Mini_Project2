from bsddb3 import db 

rwfile = 'rw.idx'
rtfile = 'rt.idx'
ptfile = 'pt.idx'
scfile = 'sc.idx' 

rwDB = db.DB()
rtDB = db.DB()
ptDB = db.DB()
scDB = db.DB()

# Searches particial terms on given index 
# Input : start result, last result, result set, indx
# Return: result set
def search_similar (start_b,last_b,rec_id,ind):
	cur_start= ind.cursor()
	cur_end= ind.cursor()
	result = cur_start.set_range(start_b)
#	print("start : ",result)
	last = cur_end.set_range(last_b)
#	print("end : ",last)

	while(result!=last):
		rec_id.add(result[1])
		print(result)
		result = cur_start.next()
#	print("Similar terms set :",rec_id)
	return rec_id

def search_p(key):
	"""
	Search for product title terms that exactly match user string
	Input: user entered string
	Return: set of record id's
	"""
	# check for %
	# Which we're not doing for now
	rec_ids = set()

	if key.find("%") != -1:
	 	key = key.strip("%")
	 	end = key[:-1]+ chr(ord(key[-1])+1)

	 	key = bytes(key,'ascii')
	 	end = bytes(end,'ascii')
	 	rec_ids = search_similar(key,end,rec_ids,ptDB)
	 	print(rec_ids)
	else:
		key = bytes(key, 'ascii')
		cur = ptDB.cursor()
		# find first key,data pair with matching key and 
		#set the cursor to that location
		result = cur.set(key)
		#print("first product title term result : ",result)
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
				#print("product title result :",result)
				amount = amount-1

			#print(" product title set : ",rec_ids)
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
	# HAHA NEVERMIND the beautiful Alex got it all figured out

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
		#print(" first review term result : ",result)
		if (result != None):
			amount = cur.count()-1
			rec_ids.add(result[1])
			while(amount > 0):
				result = cur.next()
				rec_ids.add(result[1])
				#print(" a review term result : ",result)
				amount = amount-1
			#print("review term result set  : ",rec_ids)
		else:
			print("Search returned no result")
	return rec_ids
	
	# Finds all record ids that have a lower score then "score"
# Uses set_range to finds lowest index with the same or barely greater 
# value then the given score.
# Move back one to find first lower score.
# Continue moving backwards until you get "NULL" also know as the beginning the end of the index
def sc_less(score):
	rec_id = set()
	print("Score : ",score)
	score = bytes(score,'ascii')
	cur = scDB.cursor()

	result = cur.set_range(score)
	#print("original result :",result)

	result = cur.prev()

	if (result == None):
		print("No result")
	else:
		while(result != None):
			rec_id.add(result[1])
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

