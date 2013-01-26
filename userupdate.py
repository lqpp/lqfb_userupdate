#!/usr/bin/python

import psycopg2
import sys
import csv
import datetime
import logging

#######################################################################################
## Config
dbname="lqfb_ap"

# create logger
logger = logging.getLogger('userupdate')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# logger file output
f2 = logging.Formatter('%(asctime)s - %(message)s')
errorlog = logging.FileHandler("error.log")
errorlog.setLevel(logging.ERROR)
errorlog.setFormatter(f2)
logger.addHandler(errorlog)

auditlog = logging.FileHandler("audit.log")
auditlog.setLevel(logging.INFO)
auditlog.setFormatter(f2)
logger.addHandler(auditlog)

# add ch to logger
logger.addHandler(ch)

# parses csv file and returns array with data
def parse_file(filename):

	file = open(filename, 'rt')

	reader = None
	users = []

	try:
		reader = csv.reader(file)

		for row in reader:

			# invite, id and identification
			user = [row[0], row[1], row[2]]

			# get all units, try to split all fields
			units = []

			for i in range(3, len(row)) :
				unit = row[i].split(',')

				for u in unit :
					units.append(u)			

			user.append(units)
			users.append(user)
		
	except:
		logger.error("Error parsing csv")
		sys.exit(1)

	finally:
		file.close()

	#sort users by invite_code
	return sorted(users)

# reads all user_ids and invite keys from lqfb database
def read_db():

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

def lock_user (user) :

	con = None

	try:	     
		con = psycopg2.connect(database = dbname, user = 'www-data') 
		cur = con.cursor()

		cur.execute("BEGIN")
		cur.execute("UPDATE member set (locked, active) = (true,false) where invite_code=\'"  + user[0] + "\'")
		cur.execute("COMMIT") 


	except psycopg2.DatabaseError, e:
		logger.error("DB Error: %s" % e)
		sys.exit(1)	    
	    
	finally:
		if con:
			con.close()

	logger.info("Locked user: " + user[0])


def create_user (user) :

	logger.info("Created user: " + user[0])

def update_user (db_user, csv_user) :

	logger.info("Updated user: " + db_user[0])

# main program
csv_users = parse_file("test.csv")
db_users = read_db()

# db_users: [invite,user_id,identification,locked]
# csv_users = [invite, identification, Land, Kreis, Regional, darf abstimmen]


# loop through the users in the DB and check if they are in the csv
csv_index = 0

for user in db_users :

	csv_start = csv_index

	# find user in csv
	while csv_index < len(csv_users) and user[0] != csv_users[csv_index][0] :
		csv_index += 1 

	if csv_index < len(csv_users) and user[0] == csv_users[csv_index][0] :
		# we found the user
		update_user(user, csv_users[csv_index])		
		csv_users.pop(csv_index)

	else :
		# user is not in the csv so lock him
		csv_index = csv_start
		lock_user(user)

# create users
for user in csv_users :
	create_user(user)

#logger.info(csv_users)
#logger.info(db_users)
