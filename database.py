#!/usr/bin/env python3
import psycopg2

#####################################################
##  Database Connection
#####################################################

'''
Connect to the database using the connection string
'''
def openConnection():
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y21s1c9120_gluo3599"
    passwd = "500321128"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"

    # Create a connection to the database
    conn = None
    try:
        # Parses the config file and connects using the connect string
        conn = psycopg2.connect(database=userid,
                                    user=userid,
                                    password=passwd,
                                    host=myHost)
    except psycopg2.Error as sqle:
        print("psycopg2.Error : " + sqle.pgerror)
    
    # return the connection to use
    return conn

'''
Validate a sales agent login request based on username and password
'''
def checkUserCredentials(userName, password):
    # TODO - validate and get user info for a sales agent
    userInfo=None
    conn=openConnection()
    try:
        curs=conn.cursor()
        
        #execute the query
        curs.callproc('validUser', (userName,password))
        
        row = curs.fetchone()
        if curs.rowcount != 0:
            userInfo=[str(row[0]),str(row[1]),str(row[2]),str(row[3]),str(row[4])]

        #clean up
        curs.close()

    except psycopg2.Error as sqle:       
        #TODO: add error handling #/
        print("There are some errors.")
        print(sqle)
        
    conn.close()
    return userInfo


'''
List all the associated bookings in the database for a given sales agent Id
'''
def findBookingsBySalesAgent(agentId):
    # TODO - list all the associated bookings in DB for a given sales agent Id
    conn=openConnection()
    booking_list=None
    try:   
        curs=conn.cursor()
        
        #excute the query
        curs.execute("""select BOOKING_NO, c.FIRSTNAME, c.LASTNAME, PERFORMANCE, PERFORMANCE_DATE, a.FIRSTNAME, a.LASTNAME, INSTRUCTION
        from BOOKING join AGENT a on BOOKED_BY = AGENTID join CUSTOMER c on CUSTOMER=EMAIL 
        where BOOKED_BY = %s order by c.FIRSTNAME, c.LASTNAME""",(agentId,))
        
        rows=curs.fetchall()    
        if curs.rowcount != 0:
            booking_list = [{
            'booking_no': str(row[0]),
            'customer_name': row[1]+" "+row[2],
            'performance': row[3],
            'performance_date': row[4],
            'booked_by': row[5]+" "+row[6],
            'instruction': row[7]
            } for row in rows]
                 
        # clean up!
        curs.close()
        
    except psycopg2.Error as sqle:       
        #TODO: add error handling #/
        print("There are some errors.")
    
    conn.close()
    return booking_list


'''
Find a list of bookings based on the searchString provided as parameter
See assignment description for search specification
'''
def findBookingsByCustomerAgentPerformance(searchString):
    conn=openConnection()
    booking_list=None
    searchString = '%'+ searchString
    searchString += '%'
    try:   
        curs=conn.cursor()


        args=[searchString, searchString, searchString, searchString, searchString]
        sql="""select DISTINCT BOOKING_NO, c.FIRSTNAME, c.LASTNAME, PERFORMANCE, PERFORMANCE_DATE, a.FIRSTNAME, a.LASTNAME, INSTRUCTION
                        from BOOKING join AGENT a on BOOKED_BY = AGENTID join CUSTOMER c on CUSTOMER=EMAIL 
                        where UPPER(a.firstname) LIKE UPPER(%s) 
                            OR UPPER(a.lastname) LIKE UPPER(%s) 
                            OR UPPER(c.firstname) LIKE UPPER(%s) 
                            OR UPPER(c.lastname) LIKE UPPER(%s) 
                            OR UPPER(performance) LIKE UPPER(%s) 
                        order by c.FIRSTNAME, c.LASTNAME"""
        curs.execute(sql,args)
        rows=curs.fetchall()
        if curs.rowcount != 0:
            booking_list = [{
            'booking_no': str(row[0]),
            'customer_name': row[1]+" "+row[2],
            'performance': row[3],
            'performance_date': row[4],
            'booked_by': row[5]+" "+row[6],
            'instruction': row[7]
            } for row in rows]
        curs.close()

    except psycopg2.Error as sqle:       
        #TODO: add error handling #/
        print("There are some errors.")
    
    conn.close()
    return booking_list

#####################################################################################
##  Booking (customer, performance, performance date, booking agent, instruction)  ##
#####################################################################################
'''
Add a new booking into the database - details for a new booking provided as parameters
'''
def addBooking(customer, performance, performance_date, booked_by, instruction):
    # TODO - add a booking
    # Insert a new booking into database
    # return False if adding was unsuccessful
    # return True if adding was successful
    conn=openConnection()
    try:   
        curs=conn.cursor()
        agent_id = -1
        curs.execute("""select AGENTID
                        from AGENT
                        where USERNAME = %s""", (booked_by,))
        
        if curs.rowcount != 0:
            agent_id = curs.fetchall()[0][0]
        else:
            return False

        if agent_id == -1:
            return False

        curs.execute("""INSERT INTO BOOKING(CUSTOMER, PERFORMANCE, PERFORMANCE_DATE, BOOKED_BY, INSTRUCTION) VALUES (%s,%s,%s,%s,%s)""", 
            (customer, performance, performance_date, agent_id, instruction,))
        conn.commit()
        # clean up!
        curs.close()
        conn.close()
        return True
        
    except psycopg2.Error as sqle:
        #TODO: add error handling #/
        print("There are some errors.")
        print(sqle)
        return False
    finally:
        conn.close()


'''
Update an existing booking with the booking details provided in the parameters
'''
def updateBooking(booking_no, performance, performance_date, booked_by, instruction):
    conn=openConnection()
    try:   
        curs=conn.cursor()
        agent_id = -1
        curs.execute("""select AGENTID
                        from AGENT
                        where USERNAME = %s""", (booked_by,))
        
        if curs.rowcount != 0:
            agent_id = curs.fetchall()[0][0]
        else:
            return False

        if agent_id == -1:
            return False

        curs.execute("""UPDATE BOOKING
                    SET PERFORMANCE = %s, PERFORMANCE_DATE = %s, BOOKED_BY = %s, INSTRUCTION = %s
                    WHERE BOOKING_NO = %s""", 
                    (performance, performance_date, agent_id, instruction, booking_no,))
        conn.commit()
        # clean up!
        curs.close()
        conn.close()
        return True
        
    except psycopg2.Error as sqle:
        #TODO: add error handling #/
        print("Panic! There are some errors.")
        print(sqle)
        return False
    finally:
        conn.close()