#!/usr/bin/python

import psycopg2
import sys
import csv
import datetime
import logging
import os

#######################################################################################
## Config
dbname="lqfb_ap"
#######################################################################################

#################
# Option Parser
#################
from optparse import OptionParser

usage = "usage: %prog [options]\n\nApply a whitelist of invitecodes to the LQFB database."
parser = OptionParser(usage)
parser.add_option('-d', '--dry-run',  action='store_true',dest="dryrun", default=False, help='No changes to database, proposed changes written to logfile.')
parser.add_option('-u', '--user-names',  action='store_true',dest="usernames", default=False, help='Show the display names of locked users (radomized order)')
parser.add_option('-a', '--list-active',  action='store_true',dest="list_active", default=False, help='Shows a seperate list of all locked invite codes whose accounts are active')

(options, args) = parser.parse_args()

###########
# Logger
###########
os.remove('audit.log')
os.remove('error.log')

# create logger
logger = logging.getLogger('userupdate')
logger.setLevel(logging.DEBUG)

# error log
fe = logging.Formatter('%(levelname)s - %(message)s')
errorlog = logging.FileHandler("error.log")
errorlog.setLevel(logging.ERROR)
errorlog.setFormatter(fe)
logger.addHandler(errorlog)

# audit log
fa = logging.Formatter('%(message)s')
auditlog = logging.FileHandler("audit.log")
auditlog.setLevel(logging.INFO)
auditlog.setFormatter(fa)
logger.addHandler(auditlog)

# console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(fe)
logger.addHandler(console)


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
def read_db(dbname):

	con = None
	users = None

	try:	     
		con = psycopg2.connect(database = dbname, user = 'www-data') 
		cur = con.cursor()

		cur.execute("SELECT invite_code,id,identification,locked,name,active FROM member ORDER BY invite_code")

		users=cur.fetchall()	    

	except psycopg2.DatabaseError, e:
		logger.error('Error reading db: %s' % e)
		sys.exit(1)	    
	    
	finally:
		if con:
			con.close()

	return users

def lock_users (db_users) :
	
	con = None

	try:	     
		con = psycopg2.connect(database = dbname, user = 'www-data') 
		cur = con.cursor()

		cur.execute("BEGIN")

		logger.info("\n######################################################\nLocked invite codes: ")

		for db_user in db_users:
			
			#check that invitecode is nonempty
			if db_user[0] != None :

				cur.execute("UPDATE member set (locked, active) = (true,false) where invite_code=\'"  + db_user[0] + "\'")				

				logger.info(db_user[0])

			else :		
				logger.error("Error locking user: Empty invite code: ID=" + str(db_user[1]) )

		if options.dryrun :
			cur.execute("ABORT")
		else :
			cur.execute("COMMIT") 

	except psycopg2.DatabaseError, e:
		logger.error("DB Error: %s" % e)
		sys.exit(1)	    
	    
	finally:
		if con:			
			con.close()


def create_users (csv_users) :
	con = None

	try:	     
		con = psycopg2.connect(database = dbname, user = 'www-data') 
		cur = con.cursor()

		cur.execute("BEGIN")

		logger.info("\n######################################################\nCreated invite codes: ")

		for csv_user in csv_users:
			
			#check that invitecode is nonempty
			if csv_user[0] != None :

				# check if invite code already exists
				cur.execute("SELECT id FROM member WHERE invite_code=\'" + csv_user[0] + "\'" )
				invite=cur.fetchall()			

				if len(invite) == 0 :

					cur.execute("INSERT INTO member (invite_code, locked, active, admin) VALUES (\'" + csv_user[0] + "\', false, false, false)")
					logger.info(csv_user[0])

				else :
					logger.error("Error creating user: the following user already has this invite code: ID=" + str(invite[0]) )

			else :		
				logger.error("Error creating user: empty invite code. User: " + str(csv_user) )

		if options.dryrun :
			cur.execute("ABORT")
		else :
			cur.execute("COMMIT") 

	except psycopg2.DatabaseError, e:
		logger.error("DB Error: %s" % e)
		sys.exit(1)	    
	    
	finally:
		if con:			
			con.close()	

	

def update_user (db_user, csv_user) :

	#identification
	#units


	logger.info("Updated user: " + db_user[0])

# main program
logger.info("Time: " + str(datetime.datetime.now()))

if options.dryrun :
	logger.info("**This is a dryrun: The changes are not applied**")

csv_users = parse_file("test.csv")
db_users = read_db(dbname)

# db_users: [invite, user_id, identification, locked, name, active ]
# csv_users = [invite, identification, darf abstimmen, [units]]

csv_users_match = []
db_users_del = []
csv_users_del = []
# loop through the users in the DB and check if they are in the csv
for i, db_user in enumerate(db_users) :

	for j, csv_user in enumerate(csv_users) :
		if db_user[0] == csv_user[0] :			
			csv_users_match.append(csv_user)
			db_users_del.append(i)
			csv_users_del.append(j)
			break

for i in sorted(db_users_del, reverse=True) :
	del db_users[i]

for i in sorted(csv_users_del, reverse=True) :
	del csv_users[i]

# all users left in db_users have to be locked, remove all that already are
db_users_lock = [db_user for db_user in db_users if db_user[3] == False ]

# lock all remaining users
if len(db_users_lock) > 0 :
	lock_users(db_users_lock)

# create new users
if len(csv_users) :
	create_users(csv_users)

if options.list_active :
	logger.info("\n######################################################\nLocked accounts that were active")
	for db_user in db_users :
		if db_user[5] == True :
			logger.info(db_user[0])


if options.usernames :
	logger.info("\n######################################################\nDisplay names of locked accounts (randomized)")
	import random
	random.shuffle(db_users,random.random)
	for db_user in db_users :
		if db_user[4] != None :
			logger.info(db_user[4])



# create users
#for user in csv_users :
#	create_user(user)


### debugging stuff/ delete when finished

#logger.info(csv_users)
#logger.info(db_users)
