#!/usr/bin/python

import psycopg2
import sys
import csv

# parses csv file and returns array with data
def parse_file(filename):

	file = open(filename, 'rt')

	reader = None
	users = []

	try:
		reader = csv.reader(file)

		for row in reader:
			users.append(row)
		
	finally:
		file.close()

	#sort users by invite_code

	return sorted(users)

# reads all user_ids and invite keys from lqfb database
def read_db(dbname):

	con = None
	users = None

	try:	     
		con = psycopg2.connect(database = dbname, user = 'www-data') 
		cur = con.cursor()

		cur.execute("SELECT invite_code,id,identification FROM member ORDER BY invite_code")

		users=cur.fetchall()	    

	except psycopg2.DatabaseError, e:
		print 'Error %s' % e    
		sys.exit(1)	    
	    
	finally:
		if con:
			con.close()

	return users



# main program
csv_users = parse_file("test.csv")
db_users = read_db("lqfb_ap")

# db_users: [invite,user_id,identification]
# csv_users = [invite, identification, Land, Kreis, Regional, darf abstimmen]


# loop through the users in the DB and check if they are in the csv
for user in db_users :
	
	





print csv_users
print db_users