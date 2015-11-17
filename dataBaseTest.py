from bsddb3 import db   
filename = 'pt.idx'

ptermsDB = db.DB()

ptermsDB.open(filename)
cursor = ptermsDB.cursor()

rec = cursor.first()
while rec:
        print (rec)
        rec = cursor.next()


print("Find Camera")

result = cursor.get(b'nun',db.DB_FIRST)

print(result)


ptermsDB.close()
