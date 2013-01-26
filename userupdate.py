#!/usr/bin/python

import psycopg2
import sys
import csv
import datetime

logfile = "userupdate_log_" + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S") + '.log'

# parses csv file and returns array with data
def parse_file(filename):

	file = open(filename, 'rt')

	reader = None
	users = []

	try:
		reader = csv.reader(file)

		for row in reader:
			users.append(row)
		
	except:
		sys.exit(1)

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

		cur.execute("SELECT invite_code,id,identification,locked FROM member ORDER BY invite_code")

		users=cur.fetchall()	    

	except psycopg2.DatabaseError, e:
		print 'Error %s' % e    
		sys.exit(1)	    
	    
	finally:
		if con:
			con.close()

	return users

def logger(text) :
	
	log=open(logfile, 'a')

	time=datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

	log.write(time + ": " + text)

	print text

	log.close()
	
def lock_user (user) :

	logger("Locked user: " + user[0])


def create_user (user) :

	logger("Created user: " + user[0])

def update_user (db_user, csv_user) :

	logger("Updated user: " + db_user[0])



# main program
csv_users = parse_file("test.csv")
db_users = read_db("lqfb_ap")

# db_users: [invite,user_id,identification,locked]
# csv_users = [invite, identification, Land, Kreis, Regional, darf abstimmen]


# loop through the users in the DB and check if they are in the csv
csv_index = 0

for db_index in len(db_users) :

	user = db_users[db_index]

	csv_start = csv_index

	# find user in csv
	while user[0] != csv_users[csv_index][0] :

		csv_index += 1 


	if user[0] == csv_users[csv_index][0] :
		# we found the user
		update_user(user, csv_users[csv_index])

	else :
		# user is not in the csv so lock him
		csv_index = csv_start
		lock_user(user)








#print csv_users
#print db_users