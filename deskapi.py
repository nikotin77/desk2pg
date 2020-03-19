#   Author:         Shiryaev Evgeny
#   Version:        1.0
#   Created:        19.03.2020
#   Updated:        19.03.2020
#   Description:    Functions for desk API
#  

from __future__ import print_function
import datetime, json, sys
import logging

module_logger = logging.getLogger("desk2pg.desk")

# Log function
def log(lvl, msg):
    #print(lvl, msg)
    logger = logging.getLogger("desk2pg.desk")
    if lvl == "INFO":
        logger.info(msg)
    elif lvl == "ERROR":
        logger.error(msg) 

try:
    import requests
except ImportError:
    log("ERROR", "It looks like you don't have the requests module installed.")    
    log("ERROR", "This can be installed with:")
    log("ERROR", "  $ pip install requests")
    log("ERROR", "Also see the requests documentation:")
    log("ERROR", "  http://docs.python-requests.org/en/master/user/install/")    
    sys.exit(1)

# Helper methods so we don't have to add the installation & authentication
def _get(u, **p):    return requests.get(install + u, auth=(key, ''), **p)

# Get ticket list from teamwork.com
def get_tickets(pageNum, pageSize):
    url = '/desk/v1/tickets/search.json?pageSize={}&page={}'.format(pageSize, pageNum)
    log("INFO", url)
    r = _get(url)
    if r.status_code != 200:        
        log('ERROR', r)
        quit()
        
    return r.json()

# Get one ticket from teamwork.com
def get_ticket(ticket_id):
    log("INFO", '/desk/v1/tickets/%s.json'%(ticket_id))
    r = _get('/desk/v1/tickets/{}.json'.format(ticket_id))
    if r.status_code != 200:
        log('ERROR', r)
        quit()
        
    return r.json()


if __name__ == '__main__':
    print(help(__name__))


