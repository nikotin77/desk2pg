#   Author:         Shiryaev Evgeny
#   Version:        1.0
#   Created:        19.03.2020
#   Updated:        19.03.2020
#   Description:    Functions for postgres database
#  

import psycopg2
import logging

module_logger = logging.getLogger("desk2pg.pgdb")

# Log function
def log(lvl, msg):
    #rint(lvl, msg)
    logger = logging.getLogger("desk2pg.pgdb")    
    if lvl == "INFO":
        logger.info(msg)
    elif lvl == "ERROR":
        logger.error(msg)  
# Create connection to postgres
def connect(dbconfig):
    try:
        log('INFO', 'Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host     = dbconfig['HOST'], 
            dbname   = dbconfig['DATABASE'], 
            user     = dbconfig['USER'], 
            password = dbconfig['PASSWORD']
        )    
        cur = conn.cursor()            
        log('INFO', 'PostgreSQL database version:')
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        log('INFO', db_version)       
        cur.close()
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        log('ERROR', error)
        return -1
# Drop connection
def disconnect(conn):
    if conn is not None:
        conn.close()
        log('INFO', 'Database connection closed.')
# Drop tables
def dropTables(conn):
    sql = """
        drop table if exists public.contact;
        drop table if exists public.tag;
        drop table if exists public.thread;
        drop table if exists public.ticket;
        drop table if exists public.customer;
    """
    try:        
        cur = conn.cursor()        
        cur.execute(sql)
        conn.commit()
        log("INFO", "Tables dropped")
        return 0
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        log("ERROR", error)
        return -1
# Create new tables
def createTables(conn):
    sql = """
        CREATE TABLE public.customer
        (
            "customerId" integer NOT NULL,
            "firstName" character varying(100),
            "lastName" character varying(100),
            "email" character varying(100),
            "notes" text,
            PRIMARY KEY ("customerId")
        )
        WITH (
            OIDS = FALSE
        );

        CREATE TABLE public.contact
        (
            "contactId" integer NOT NULL,
            "type" character varying(100),
            "value" character varying(100),
            "customerId" integer,
            PRIMARY KEY ("contactId"),
            FOREIGN KEY ("customerId")
            REFERENCES customer ("customerId")
            ON UPDATE CASCADE ON DELETE CASCADE
        )
        WITH (
            OIDS = FALSE
        );

        CREATE TABLE public.ticket
        (
            "ticketId" integer NOT NULL,
            "averageResponseTime" numeric,
            "firstResponseTime" numeric,
            "responseCount" numeric,
            "resolutionTime" numeric,
            "assignedTo" integer,
            "preview" text,
            "source" text,
            "status" text,	
            "type" text,
            "numThreads" integer,
            "createdBy" integer,
            "createdAt" timestamp without time zone,
            "updatedAt" timestamp without time zone,
            "customerId" integer,
            "subject" text,
            PRIMARY KEY ("ticketId"),
            FOREIGN KEY ("createdBy")
            REFERENCES customer ("customerId")
            ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY ("customerId")
            REFERENCES customer ("customerId")
            ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY ("assignedTo")
            REFERENCES customer ("customerId")
            ON UPDATE CASCADE ON DELETE CASCADE
        )
        WITH (
            OIDS = FALSE
        );

        CREATE TABLE public.tag
        (
            "tagId" integer NOT NULL,
            "tagName" character varying(100),
            "ticketId" integer,
            PRIMARY KEY ("tagId"),
            FOREIGN KEY ("ticketId")
            REFERENCES ticket ("ticketId")
            ON UPDATE CASCADE ON DELETE CASCADE
        )
        WITH (
            OIDS = FALSE
        );

        CREATE TABLE public.thread
        (
            "threadId" integer NOT NULL,
            "ticketId" integer,
            "body" text,
            "createdBy" integer,
            "createdAt" timestamp without time zone,
            "updatedAt" timestamp without time zone,
            PRIMARY KEY ("threadId"),
            FOREIGN KEY ("ticketId")
            REFERENCES ticket ("ticketId")
            ON UPDATE CASCADE ON DELETE CASCADE
        )
        WITH (
            OIDS = FALSE
        );
    """
    try:        
        cur = conn.cursor()        
        cur.execute(sql)
        conn.commit()
        log("INFO", "New tables created")
        return 0
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        log("ERROR", error)
        return -1
# Save tag
def insertTag(conn, tagId, tagName, ticketId):
    sql = """INSERT INTO public.tag ("tagId", "tagName", "ticketId") VALUES(%s, %s, %s) ON CONFLICT ("tagId") DO nothing returning 1;"""
    try:        
        cur = conn.cursor()        
        cur.execute(sql, (tagId, tagName, ticketId))        
        res = cur.fetchone()
        conn.commit()
        if hasattr(res, "__getitem__"):
            log("INFO", "New tag (id=%s) created"%tagId)
    except (Exception, psycopg2.DatabaseError) as error:
        log("ERROR", error)
        conn.rollback()
        return -1
# Save customer
def insertCustomer(conn, customerId, firstName, lastName, email, notes):
    sql = """INSERT INTO public.customer ("customerId", "firstName", "lastName", "email", "notes") 
            VALUES(%s, %s, %s, %s, %s)
            ON CONFLICT ("customerId") do nothing
            returning 1;"""
    try:        
        cur = conn.cursor()        
        cur.execute(sql, (customerId, firstName, lastName, email, notes))
        res = cur.fetchone()
        conn.commit()
        if hasattr(res, "__getitem__"):
            log("INFO", "New customer (id=%s) created"%customerId)        
    except (Exception, psycopg2.DatabaseError) as error:
        log("ERROR", error)
        conn.rollback()
        return -1
# Save contact
def insertContact(conn, contactId, contactType, value, customerId):
    sql = """INSERT INTO public.contact ("contactId", "type", "value", "customerId") 
            VALUES(%s, %s, %s, %s)
            ON CONFLICT ("contactId") 
            DO nothing;"""
    try:
        cur = conn.cursor()
        cur.execute(sql, (contactId, contactType, value, customerId))
        conn.commit()
        log("INFO", "New contact (id=%s) created"%contactId)
    except (Exception, psycopg2.DatabaseError) as error:
        log("ERROR", error)
        conn.rollback()
        return -1
# Save thread
def insertThread(conn, threadId, ticketId, body, createdBy, createdAt, updatedAt):
    sql = """INSERT INTO public.thread ("threadId", "ticketId", "body", "createdBy", "createdAt", "updatedAt") 
            VALUES(%s, %s, %s, %s, %s, %s)
            ON CONFLICT ("threadId") 
            DO nothing;"""
    try:
        cur = conn.cursor()
        cur.execute(sql, (threadId, ticketId, body, createdBy, createdAt, updatedAt))
        conn.commit()
        log("INFO", "New thread (id=%s) created"%threadId)
    except (Exception, psycopg2.DatabaseError) as error:
        log("ERROR", error)
        conn.rollback()
        return -1
# Save ticket
def insertTicket(conn, ticketId,
                averageResponseTime,
                firstResponseTime,
                responseCount,
                resolutionTime,
                assignedTo,
                preview,
                source,
                status,	
                ticket_type,
                numThreads,
                createdBy,
                createdAt,
                updatedAt,
                customerId,
                subject):
    sql = """INSERT INTO public.ticket (  
                "ticketId",
                "averageResponseTime",
                "firstResponseTime",
                "responseCount",
                "resolutionTime",
                "assignedTo",
                "preview",
                "source",
                "status",	
                "type",
                "numThreads",
                "createdBy",
                "createdAt",
                "updatedAt",
                "customerId",
                "subject"
            ) 
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""    
    try:        
        cur = conn.cursor()        
        cur.execute(sql, 
                    (
                        ticketId, averageResponseTime, firstResponseTime, responseCount, resolutionTime, assignedTo,
                        preview, source, status, ticket_type, numThreads, createdBy, createdAt, updatedAt, customerId, subject
                    )
        )
        conn.commit()
        log("INFO", "New ticket (id=%s) created"%ticketId)
    except (Exception, psycopg2.DatabaseError) as error:
        log("ERROR", error)
        conn.rollback()
        return -1


if __name__ == '__main__':
    print(help(__name__))
