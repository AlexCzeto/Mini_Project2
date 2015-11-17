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
	rec_ids = set()
	cur = ptDB.cursor()
# find first key,data pair with matching key and set the cursor to that location
	result = cur.set(key)
	#print(result)
	if (result != None):
# The cursor will move forward in relation to how many keys matched the string
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

# Search for review terms that exactlty match string
# Input: user entered string
# Return: set of record id's
def search_revt(key):
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

# Take set of record keys and prints them 		
def find_results(rec_ids):
	if (rec_ids):
		while(rec_ids):
			key = rec_ids.pop()
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
	byte = bytes(string, 'ascii')
	
	print("---------------------------------------------------------------------------------------------")
	print("\nResults of search by review text\n")
	rec_ids1 = search_revt(byte)
	rec_ids = set(rec_ids1)

	find_results(rec_ids1)

	print("---------------------------------------------------------------------------------------------")
	print("\nResults of search by title\n")
	rec_ids2 = search_prodt(byte)
	rec_ids = rec_ids.union(rec_ids2)
	find_results(rec_ids2)

	print("---------------------------------------------------------------------------------------------")
	print("\nResult of OR title and review terms")
	print(rec_ids)


if __name__ == "__main__":
    main()
