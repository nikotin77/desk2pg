#   Author:         Shiryaev Evgeny
#   Version:        1.0
#   Created:        19.03.2020
#   Updated:        19.03.2020
#   Description:    Main file
#   

import deskapi
import configparser
import psycopg2
import pgdb
import logging

# Get config from config.txt
config = configparser.ConfigParser()
config.read('config.txt')

# Create logger
logger = logging.getLogger("desk2pg")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(config["LOG"]["FILENAME"])
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# Log function
def log(lvl, msg):
    #print(lvl, msg)    
    if lvl == "INFO":
        logger.info(msg)
    elif lvl == "ERROR":
        logger.error(msg) 

log("INFO", "Program start")

# Set parameters for deskapi
deskapi.install = config["DESK"]["URL"]
deskapi.key = config["DESK"]["API_KEY"]

# Create postgres connection
conn = pgdb.connect(config["POSTGRES"])

if conn == -1:    
    quit()

pgdb.dropTables(conn)
pgdb.createTables(conn)

# Set count tickets for one request (page)
pageSize = 10

maxPages = 1
ticketDoneCnt = 0
pageNum = 1

# Loop pages
while pageNum <= maxPages:
    # Get ticket list
    ticketList = deskapi.get_tickets(pageNum, pageSize)
    if pageNum == 1:        
        ticketCount = ticketList["count"]
        log("INFO", "Ticket count = %s" % ticketCount)
        maxPages = ticketList["maxPages"]
        log("INFO", "Pages count = %s"%maxPages)
    
    log("INFO", "Page #%s of %s" % (pageNum, maxPages))    
    
    # Loop over tickets on the page
    for ticketNum in ticketList["tickets"]:
        log("INFO", "Get ticket #%s --------------------------------------------------" % ticketNum["id"])
        # Get ticket 
        ticket = deskapi.get_ticket(ticketNum["id"])["ticket"]
        assignedTo = None
        createdByUser = None
        if hasattr(ticket["customer"], "__getitem__"):
            # Save customer
            pgdb.insertCustomer(
                conn,
                customerId = ticket["customer"]["id"], 
                firstName  = ticket["customer"]["firstName"], 
                lastName   = ticket["customer"]["lastName"], 
                email      = ticket["customer"]["email"], 
                notes      = ticket["customer"]["notes"]
            )
            
            if hasattr(ticket["customer"]["contacts"], '__getitem__'):
                # Loop over customer contacts
                for contact in ticket["customer"]["contacts"]:                    
                    # Save contact
                    pgdb.insertContact(
                        conn,
                        contact["id"],
                        contact["type"],
                        contact["value"],
                        ticket["customer"]["id"]
                    )

        if hasattr(ticket["assignedTo"], '__getitem__'):
            assignedTo = ticket["assignedTo"]["id"]
            # Save customer (assignedTo)
            pgdb.insertCustomer(
                conn,
                customerId  = ticket["assignedTo"]["id"], 
                firstName   = ticket["assignedTo"]["firstName"], 
                lastName    = ticket["assignedTo"]["lastName"], 
                email       = ticket["assignedTo"]["email"], 
                notes       = ticket["assignedTo"]["notes"]
            )
        if hasattr(ticket["createdByUser"], '__getitem__'):
            createdByUser = ticket["createdByUser"]["id"]
            # Save customer (createdByUser)
            pgdb.insertCustomer(
                conn,
                customerId  = ticket["createdByUser"]["id"], 
                firstName   = ticket["createdByUser"]["firstName"], 
                lastName    = ticket["createdByUser"]["lastName"], 
                email       = ticket["createdByUser"]["email"], 
                notes       = ticket["createdByUser"]["notes"]
            )
        # Save ticket
        pgdb.insertTicket(
            conn,
            ticketId            = ticket["id"],
            averageResponseTime = ticketNum["responseTimes"]["averageResponseTime"],
            firstResponseTime   = ticketNum["responseTimes"]["firstResponseTime"],
            responseCount       = ticketNum["responseTimes"]["responseCount"],
            resolutionTime      = ticketNum["responseTimes"]["resolutionTime"],
            assignedTo          = assignedTo,
            preview             = ticketNum["preview"],
            source              = ticket["source"],
            status              = ticket["status"],
            ticket_type         = ticket["type"],
            numThreads          = ticketNum["numThreads"],
            createdBy           = createdByUser,            
            createdAt           = ticket["createdAt"],
            updatedAt           = ticket["updatedAt"],
            customerId          = ticket["customer"]["id"],
            subject             = ticket["subject"]
        )
        # Loop ticket tags 
        for tag in ticket["tags"]:
            # Save tag      
            pgdb.insertTag(
                conn, 
                tag["id"], 
                tag["name"], 
                ticket["id"]
            )
        # Loop ticket threads
        for thread in ticket["threads"]:
            createdByUser = None
            if hasattr(thread["createdBy"], '__getitem__'):
                createdByUser = thread["createdBy"]["id"]
                # Save customer (createdBy)
                pgdb.insertCustomer(
                    conn,
                    customerId = thread["createdBy"]["id"], 
                    firstName  = thread["createdBy"]["firstName"], 
                    lastName   = thread["createdBy"]["lastName"], 
                    email      = thread["createdBy"]["email"], 
                    notes      = thread["createdBy"]["notes"]
                )
            # Save thread
            pgdb.insertThread(
                conn,
                threadId    = thread["id"], 
                ticketId    = ticket["id"],
                body        = thread["body"],
                createdBy   = createdByUser,
                createdAt   = thread["createdAt"], 
                updatedAt   = thread["updatedAt"]
            )

        ticketDoneCnt = ticketDoneCnt + 1
        print("Progress: ticket {} of {} ({}%)".format(ticketDoneCnt, ticketCount, round(ticketDoneCnt/ticketCount*100)) )
    
    pageNum = pageNum + 1


pgdb.disconnect(conn)
log("INFO", "Done")
