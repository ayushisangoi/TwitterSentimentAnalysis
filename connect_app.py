#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import socket
import sys
import requests
import requests_oauthlib
import json
import bleach
from bs4 import BeautifulSoup
import time

# Include your Twitter account details
ACCESS_TOKEN = '1-1'
ACCESS_SECRET = '1'
CONSUMER_KEY = '1'
CONSUMER_SECRET = '1'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'	
    query_data = [('language', 'en'), ('locations', '-130,-20,100,50'),('track','iphone')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response




def send_tweets_to_spark(http_resp, tcp_connection):
    iter=0
    for line in http_resp.iter_lines():
        if iter<1000:
            try:
                full_tweet = json.loads(line)
                tweet_text = full_tweet['text']
                tweet_screen_name = full_tweet['user']['screen_name']
                source = full_tweet['source']
                soup = BeautifulSoup(source)
                for anchor in soup.find_all('a'):         
                   tweet_source = anchor.text
                source_device = tweet_source.replace(" ", "")
                device = "TS"+source_device.replace("Twitter", "")
                tweet_country_code = "CC"+full_tweet['place']['country_code']
                if tweet_country_code=="CCUS" and "tter" in tweet_source and "\n" not in tweet_text:
                   iter=iter+1 
                   print(iter)
                   print("0,0,0,0,"+tweet_screen_name+","+ tweet_text + ",end of tweet!")
                   #print(tweet_text)
                   #print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                   #print(tcp_connection)
                   fulltext="0||0||0||0||0||"+tweet_text +'\n'
                   tcp_connection.send(fulltext.encode())
            except:
                continue
        else:
            #time.sleep(20)
            break
TCP_IP ='127.0.1.1'
TCP_PORT = 9000
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind((TCP_IP, TCP_PORT))
s.listen(1)

print("Waiting for TCP connection...")
conn, addr = s.accept()

print("Connected... Starting getting tweets.")
resp = get_tweets()
#print("sending text")
#conn.send('Did it work?'.encode())
#print("sent text")
send_tweets_to_spark(resp, conn)

