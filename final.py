import random
import unittest
import pdb
import twitter_info
import json
import re
import sys
import requests
import tweepy
import sqlite3
import itertools
from collections import Counter
from collections import OrderedDict






class Movie(object):
    
    
    def __init__(self, dict1):
        self.title = dict1["Title"]
        self.director = dict1["Director"]
        self.imdb = dict1["imdbRating"]
        act = dict1["Actors"]
        self.acts = [x.strip() for x in act.split(',')]
        listactors = [x.strip() for x in act.split(',')]
        self.actors = listactors[0]
        self.id = dict1["imdbID"]
        self.language = dict1["Language"]
        self.country = dict1["Country"]
    
    def __str__(self):
        return self.title
    
    def list_of_actors(self):
        listone = self.acts
        return [x.strip() for x in listone.split(',')]

    def number_of_language(self):
        listone = self.language
        return len([x.strip() for x in listone.split(',')])




consumer_key = twitter_info.consumer_key
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Set up library to grab stuff from twitter with your authentication, and return it in a JSON format 
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

CACHE_FNAME = "SI206_final.json"



try:
    cache_file = open(CACHE_FNAME,'r')
    cache_contents = cache_file.read()
    cache_file.close()
    CACHE_DICTION = json.loads(cache_contents)
except:
    CACHE_DICTION = {}
    




def get_user_tweets(handle):
    unique_identifier = "twitter_{}".format(handle)
    if unique_identifier in CACHE_DICTION:
        twitter_results = CACHE_DICTION[unique_identifier]
    else:
        #pdb.set_trace()
        #print(handle)
        twitter_results = api.user_timeline(id=handle)
        CACHE_DICTION[unique_identifier] = twitter_results
        f = open(CACHE_FNAME,'w')
        f.write(json.dumps(CACHE_DICTION))
        f.close()
    return twitter_results

def get_item_tweets(handle):
    unique_identifier = "twitter_{}".format(handle)
    if unique_identifier in CACHE_DICTION:
        twitter_results = CACHE_DICTION[unique_identifier]
    else:
        twitter_results = api.search(handle)
        CACHE_DICTION[unique_identifier] = twitter_results
        f = open(CACHE_FNAME,'w')
        f.write(json.dumps(CACHE_DICTION))
        f.close()
        #pdb.set_trace()
        
    return twitter_results["statuses"]



def get_omdb(title):
    url = "http://www.omdbapi.com/?t="
    parameter = title
    dict1={}
    resp = requests.get(url+parameter)
    if resp.status_code ==200:
        dict1[title] = json.loads(resp.text)
#    print(type(dict1[title])
#    pdb.set_trace()
    #print(dict1[title])
    return dict1[title]

def createmovie():
    result = []
    list1 = ["friends","forrest gump","james bond"]
    for item in list1:
        dicc={}
        dicc = get_omdb(item)
        result.append(dicc)
    return result

listone = []

listone=createmovie()


movies = [Movie(i) for i in listone]

# Comments: using list comprehension to create a list of movies, this will create three movies 
# using "Friends" , "Forrest Gump","James Bond"


# Invocation to my Twitter functions 
tweetlist = []
for item in movies:
    tweets = get_item_tweets(item.actors)
    #pdb.set_trace()
    for tweet in tweets:
        tweet["movie_id"]= item.id
    
    tweetlist.append(tweets)
# The above is saved to be as the the list of tweets


# Invocation to my User Twitter Function 

userlist = []
tweets=[]

for item in tweetlist:
 
    
    for tweet in item:
        dicttw = {}
        #print(tweet.keys())
        dicttw["tweet_id"] = tweet["id"]
        dicttw["text"] = tweet["text"]
        dicttw["user_posted"] = tweet["user"]["id_str"]
        dicttw["num_favs"] = tweet["favorite_count"]
        dicttw["retweets"] = tweet["retweet_count"]
        dicttw["movie_id"] = tweet["movie_id"]
        tweets.append(dicttw)
        
        names = tweet["entities"]["user_mentions"]
        names.append(api.get_user(id=tweet["user"]["id_str"]))
        for person in names:
            # get each person who is mentioned
            dicttemp={}
            user = api.get_user(id=person["id"])
            #pdb.set_trace()
           # print(user.keys())
    #        dicttemp["user"] = api.get_user(id=person["id"])
            dicttemp["screen_name"] = user["screen_name"]
            dicttemp["num_favs"] = user["favourites_count"]
            dicttemp["description"] = user["description"]
            dicttemp["user_id"] = user["id_str"]
            userlist.append(dicttemp)
#pdb.set_tr
#Create Table for Tweets

conn = sqlite3.connect('final_tweets.db')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS Tweets')
statement = 'CREATE TABLE IF NOT EXISTS '
statement += 'Tweets(tweet_id INTEGER PRIMARY KEY, text TEXT, user_posted TEXT,movie_id INTEGER, num_favs INTEGER, retweets INTEGER)'
cur.execute(statement)

# Create Table for USers

cur.execute('DROP TABLE IF EXISTS Users')
statement1 = 'CREATE TABLE IF NOT EXISTS '
statement1 += 'Users (user_id TEXT PRIMARY KEY, screen_name TEXT, num_favs INTEGER, description TEXT)'
cur.execute(statement1)

# Create Table for Movie 

cur.execute('DROP TABLE IF EXISTS Movies')
statement2 = 'CREATE TABLE IF NOT EXISTS '
statement2 += 'Movies(movie_id TEXT PRIMARY KEY, title TEXT, director TEXT, num_language INTEGER, imdb INTEGER, top_bill TEXT, country TEXT)'
cur.execute(statement2)

# Fill the blanks 

# First is the Tweets 

statement = 'INSERT INTO Tweets VALUES(?,?,?,?,?,?)'

statement2 = 'INSERT or IGNORE INTO Users VALUES(?,?,?,?)'

statement3 = 'INSERT INTO Movies VALUES(?,?,?,?,?,?,?)'

for item in tweets:
    tweet_id = item["tweet_id"]
    text = item["text"]
    num_favs = item["num_favs"]
    retweets = item["retweets"]
    user_posted = item["user_posted"]
    movie_id = item["movie_id"]
        #statement = 'CREATE TABLE IF NOT EXISTS '
        #statement += 'Tweets(tweet_id INTEGER PRIMARY KEY, text TEXT, user_posted TEXT,movie_id INTEGER, num_favs INTEGER, retweets INTEGER)'
        #cur.execute(statement)
    #statement = 'INSERT INTO Tweets VALUES(?,?,?,?,?,?)'
    tupleone = (tweet_id,text,user_posted,movie_id,num_favs,retweets)
    cur.execute(statement,tupleone)


# fill the user table


for item in userlist:
#        pdb.set_trace()
 #       print(item.keys())
    screen_name = item["screen_name"]
    user_id = item["user_id"]
    num_favs = item["num_favs"]
    description = item["description"]
    tupletwo = (user_id,screen_name,num_favs,description)
    #statement2 = 'INSERT or IGNORE INTO Users VALUES(?,?,?,?)'
    cur.execute(statement2,tupletwo)


for item in movies:
        #pdb.set_trace()
        #print(item.number_of_language)
    #statement3 = 'INSERT INTO Movies VALUES(?,?,?,?,?,?,?)'
    tuplethree = (item.id,item.title,item.director,item.number_of_language(),item.imdb,item.actors,item.country)
    #print(tuplethree)
    cur.execute(statement3,tuplethree)
        
conn.commit()


# Process Data and make query

def makequery():


    # The first query 
    query = "SELECT description FROM Users WHERE length(screen_name) < 10"
    cur.execute(query)
    listone = cur.fetchall()
    description_length_screen = [ x[0] for x in listone]
    description_word = { y for x in description_length_screen for y in x.split()}
    long_word = [x for x in description_word if len(x) > 6]
    long_word = sorted(long_word)
    print(long_word)
    #cnt = Counter()
   
    # the longest word in the description 
    
    # the movie that has the most retweets

    

    # get the title of the movie that has been retweets most
    # second query  Inner join query

    query = "SELECT SUM(Tweets.retweets),Movies.title FROM Tweets INNER JOIN Movies ON Tweets.movie_id = Movies.movie_id GROUP BY Tweets.movie_id"
    cur.execute(query)
    listone = cur.fetchall()
#    pdb.set_trace()
    title_movie = sorted(listone)[0][1]
    #pdb.set_trace()
    print(title_movie)
    

    # total number of language with imdb rating higher than 8
    total_num_imdb = 0
    query = "SELECT title,num_language FROM Movies WHERE imdb > 8.0"
    cur.execute(query)
    listone = cur.fetchall()
    
    dict1 = dict(listone)
    for i in dict1.keys():
        total_num_imdb+=dict1[i]
    
    


    # Using Orderdict from the collection library to find the the user id which has the biggest number of favour
    query = "SELECT Users.user_id,Users.num_favs FROM Users INNER JOIN Tweets ON Tweets.user_posted = Users.user_id GROUP BY Tweets.user_posted "
    cur.execute(query)
    listone = cur.fetchall()
    dict2 = dict(listone)
    ordered = OrderedDict(sorted(dict2.items(),key=lambda t:t[1]))
    itemdict = list(ordered.items())
    biggest_value = itemdict[0][0]



makequery()







# Test Area



print("\n\nBELOW THIS LINE IS OUTPUT FROM TESTS:\n")


class Constructor1(unittest.TestCase):
    dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        
    def test_constructor(self):
        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        s = Movie(dict1)
        self.assertEqual(s.title,"the great Zhong")
    def test_str(self):
        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        m = Movie(dict1)
        self.assertEqual(print(m),"the great Zhong")
    def test_imdb(self):
        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])

        l = Movie(dict1)
        self.assertEqual(l.imdb,1.5)
    def test_country(self):

        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        p = Movie(dict1)
        self.assertEqual(p.country,"China")
    
    def test_director(self):
        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        q = Movie(dict1)
        self.assertEqual(q.director,'Shan')
    def test_num_of_language(self):
        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        w = Movie(dict1)
        self.assertTrue(w.number_of_language()==1)
    def test_list_of_actors(self):

        dict1 = dict([('Actors',['adf','hsz','zhong','bob']),('Title',"the great Zhong"),('imdbRating',1.5),('imdbID',123123),('Country','China'),('Language','Chinese'),('Director','Shan')])
        e = Movie(dict1)
        self.assertTrue(len(e.list_of_actors())==4)

class Task2(unittest.TestCase):
    def test_tweets_1(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM Tweets');
        result = cur.fetchall()
        self.assertTrue(len(result)>=20, "Testing there are at least 20 records in the Tweets database")
        conn.close()
    def test_tweets_2(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM Tweets');
        result = cur.fetchall()
        self.assertTrue(len(result[1])==6, "Testing there are 6 columns in the Tweets database")
        conn.close()
    def test_tweets_3(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT user_posted FROM Tweets');
        result = cur.fetchall()
        self.assertTrue(len(result[1][0])>=2,"Testing that a tweet user_id value fulfills a requirement of being a Twitter user id rather than an integer, etc")
        conn.close()
    def test_tweets_4(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT tweet_id FROM Tweets');
        result = cur.fetchall()
        self.assertTrue(result[0][0] != result[19][0], "Testing part of what's expected such that tweets are not being added over and over (tweet id is a primary key properly)...")
        if len(result) > 20:
            self.assertTrue(result[0][0] != result[20][0])
        conn.close()
    
    def test_user_1(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM Users');
        result = cur.fetchall()
        self.assertTrue(len(result)>=20, "Testing there are at least 20 records in the Users database")
        conn.close()
    def test_tweets_2(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM Users');
        result = cur.fetchall()
        self.assertTrue(len(result[0])==4, "Testing there are 4 columns in the Users database")
        conn.close()
    def test_movie_1(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM Movies');
        result = cur.fetchall()
        self.assertTrue(len(result)==3, "Testing there are 3 records in the Movie database")        
    def test_movie_2(self):
        conn = sqlite3.connect('final_tweets.db')
        cur = conn.cursor()
        cur.execute('SELECT * FROM Movies');
        result = cur.fetchall()
        self.assertTrue(len(result[0])==7, "Testing there are 7 column in the Movie database")


if __name__ == "__main__":
    unittest.main(verbosity=2)
