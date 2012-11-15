import requests
import simplejson as json
from math import ceil
from random import choice
import sys
import csv
import HTMLParser
import logging
import time
import string

API_HOST = 'http://otter.topsy.com'
API_KEY_LIST = [] #amruta's
        
class Result(object):
    def __init__(self, request):
        self._request = request
        try:
            self._data = json.loads(request.content)
        except Exception as e:
            print e
            print 'request.content: ', request.content
            print 'request.status_code: ', request.status_code
            self.request = None
            self.response = None
            return
            
        self.request = self._data['request']
        self.response = self._data['response']
        #the mintime of tweet from the result dataset of tweets with oldest mintime 
        self.oldest_mintime = sys.maxint
        
    def sanitize_string(self, string):
        #remove new line character from string
        string = string.replace("\n"," ")
        #escape the HTML special chars 
        parser = HTMLParser.HTMLParser()
        return parser.unescape(string).encode('utf-8')
                    
    def dump_json_to_file(self, movie_name):
        list_dicts = (self.response['list'])
        file_name = movie_name + '.csv'
        csv_file =  csv.writer(open(file_name,'ab'), delimiter='|')
        #csv_file.writerow(["content", "trackback_date", "score", "trackback_author_name", "trackback_author_nick"])
        for mydict in list_dicts:
            if mydict['trackback_date'] < self.oldest_mintime:
                print mydict['trackback_date']
            self.oldest_mintime = min(self.oldest_mintime, mydict['trackback_date'])
            csv_file.writerow([self.sanitize_string(mydict['content']),mydict['trackback_date'],mydict['score'], self.sanitize_string(mydict['trackback_author_name']), self.sanitize_string(mydict['trackback_author_nick'])])
        
        
    def get_oldest_mintime(self):
        return self.oldest_mintime
        
class Topsy(object):
    def __init__(self, api_key=''):
        #randomly select from the list of APIs
        self._api_key = api_key or choice(API_KEY_LIST) 
        self._api_host = API_HOST

    def _get(self, resource='', **params):
        params['apikey'] = self._api_key
        url = '%s/%s.json' % (self._api_host, resource)
        while(1):
            try:
                r = requests.get(url, params=params)
            except Exception as e:
                print e
                logging.debug('I am sleeping for 10 secs!!!!!\n\n')
                print 'I am sleeping for 10 secs!!!!!\n\n'
                time.sleep(10)
                continue
            
            #logging.debug("x-ratelimit-remaining: " + r.headers['x-ratelimit-remaining'])
            logging.debug(r.headers)
            result = Result(request=r)
            if result.response is None:
                logging.debug('I am sleeping for 10 secs!!!!!\n\n')
                print 'I am sleeping for 10 secs!!!!!\n\n'
                time.sleep(10)
                continue
            else:
                break
        return result
    
    def search_helper(self, q, page, perpage, maxtime, mintime):
        return self._get('search', q=q, allow_lang='en', type='tweet', page=page, perpage=perpage, maxtime=maxtime, mintime=mintime)
        
    def search_result_count(self, q):
        return self._get('searchcount', q=q, allow_lang='en', type='tweet')
        
    def search(self, movie_name, q=''):
        """
        1354337580 corresponds to Sat, 01 Dec 2012 04:53:00 GMT
        1207008000 corresponds to 1st april 2008 , 12am
        current unix time: 1350103730
        some very famous movies like 'the dark knight' can have 10k tweets in ~4 days: through topsy
        you can retrieve at max 10 pages each page with a max of 100 tweets=> max 10k tweets can be 
        downloaded in one API call.
        The diff in mintime and maxtime now is just 3 days.
        """
        #find total number of tweets posted till now
        total_num_results = (self.search_result_count(q).response)['a']
        print 'total_num_results: ' , total_num_results
        
        mintime = 1349061086 # 1st oct 2012
        maxtime = 1351742119 # 1st nov 2012
        diff = maxtime - mintime
        current_count = 0 
        #collect max of 10k tweets:
        #1226466000 is  Nov 12 2008 00:00:00 GMT-0500: don't go before this time
        while( (current_count <= total_num_results) or (current_count <= 10000) ):
            if (maxtime < 1226466000):
                break
            result = self.search_helper(q, 1, 100, maxtime, mintime)
            num_results = result.response['total']
            print 'num_results: ', num_results
            if num_results == 0:
                maxtime = mintime-86400
                mintime = maxtime - diff    
                continue
            result.dump_json_to_file(movie_name)
            if(num_results > 100):
                current_count += 100
            else:
                current_count += num_results
            oldest_min_time = result.get_oldest_mintime()
            print 'oldest_min_time: ', oldest_min_time
            num_pages_left = int(ceil((num_results-100.0)/100.0))
            print "num_pages_left: " , num_pages_left
            if num_pages_left >= 1:
                for page in range(2,num_pages_left+2):
                    #sometimes num of results may exceed 1000 in the assumed time window
                    if(page > 10):
                        break
                    print 'querying for page: ', page
                    result = self.search_helper(q, page, 100, maxtime, mintime)    
                    result.dump_json_to_file(movie_name)
                    current_count += 100
                    oldest_min_time = min(oldest_min_time, result.get_oldest_mintime())
                    print 'oldest min time for page: ', page, ": ",  (oldest_min_time)
            maxtime = oldest_min_time-86400
            mintime = maxtime - diff
            print 'new maxtime: ', maxtime , "; new mintime: ", mintime
        
def sanitize_file_name(file_name):
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits)
    valid_file_name = ''.join(c for c in file_name if c in valid_chars)
    return valid_file_name

def main():
    #iterate file Movies.txt and call collect tweet for each movie name
    with open('C:\Users\Abhishek\workspace\TBMR\Movies.txt', 'r') as movie_file:
        for movie_name in movie_file:
            #movie_name = 'Wrath of the Titans'
            print 'collecting tweets for movie: ' + movie_name
            logging.debug('collecting tweets for movie: ' + movie_name + "!!!\n\n\n")
            movie_name = movie_name.rstrip('\n')
            movie_name = sanitize_file_name(movie_name)
            log_file = 'final.log'
            logging.basicConfig(filename = log_file, filemode = 'w', level = logging.DEBUG)
            topsy = Topsy()
            #get movie name from IMDB database of Top 250 movies
            
            #modify the query as: Forrest Gump watch OR Forrest Gump watching OR Forrest Gump movie OR Forrest Gump see OR Forrest Gump saw OR Forrest Gump seeing OR Forrest Gump film
            q = movie_name + ' watch OR ' + movie_name + ' watching OR ' + movie_name + ' movie OR ' + movie_name + ' see OR ' + movie_name + ' saw OR ' + movie_name + ' seeing OR ' + movie_name + ' film '  
            topsy.search(movie_name, q)
        
    
if __name__ == '__main__':
    main()