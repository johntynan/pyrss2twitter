#!/usr/bin/env python

# System
import os
import sys
import time
import pickle
import urllib, urllib2, httplib
import hashlib
import threading, Queue
# 3rd Party
import twitter
import sqlite3
import feedparser

# Constants
DIRECT_MESSAGE_DELAY = 300 # 5 mins
RSS_FEED_DELAY = 60 # 1 min
POST_LENGTH = 124 # though twitter allows 140 characters, they seem to truncate stuff larger than about 125 or so for online display

def latin1_to_ascii(unicharacters):
	"""This takes a UNICODE string and replaces Latin-1 characters with
	something equivalent in 7-bit ASCII. It returns a plain ASCII string. 
	This function makes a best effort to convert Latin-1 characters into 
	ASCII equivalents. It does not just strip out the Latin-1 characters.
	All characters in the standard 7-bit ASCII range are preserved. 
	In the 8th bit range all the Latin-1 accented letters are converted 
	to unaccented equivalents. Most symbol characters are converted to 
	something meaningful. Anything not converted is deleted.
	
	Original code by Noah Spurrier from:
	http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/251871
	"""
	xlate={0xc0:'A', 0xc1:'A', 0xc2:'A', 0xc3:'A', 0xc4:'A', 0xc5:'A',
		0xc6:'Ae', 0xc7:'C',
		0xc8:'E', 0xc9:'E', 0xca:'E', 0xcb:'E',
		0xcc:'I', 0xcd:'I', 0xce:'I', 0xcf:'I',
		0xd0:'Th', 0xd1:'N',
		0xd2:'O', 0xd3:'O', 0xd4:'O', 0xd5:'O', 0xd6:'O', 0xd8:'O',
		0xd9:'U', 0xda:'U', 0xdb:'U', 0xdc:'U',
		0xdd:'Y', 0xde:'th', 0xdf:'ss',
		0xe0:'a', 0xe1:'a', 0xe2:'a', 0xe3:'a', 0xe4:'a', 0xe5:'a',
		0xe6:'ae', 0xe7:'c',
		0xe8:'e', 0xe9:'e', 0xea:'e', 0xeb:'e',
		0xec:'i', 0xed:'i', 0xee:'i', 0xef:'i',
		0xf0:'th', 0xf1:'n',
		0xf2:'o', 0xf3:'o', 0xf4:'o', 0xf5:'o', 0xf6:'o', 0xf8:'o',
		0xf9:'u', 0xfa:'u', 0xfb:'u', 0xfc:'u',
		0xfd:'y', 0xfe:'th', 0xff:'y',
		0xa1:'!', 0xa2:'{cent}', 0xa3:'{pound}', 0xa4:'{currency}',
		0xa5:'{yen}', 0xa6:'|', 0xa7:'{section}', 0xa8:'{umlaut}',
		0xa9:'{C}', 0xaa:'{^a}', 0xab:'<<', 0xac:'{not}',
		0xad:'-', 0xae:'{R}', 0xaf:'_', 0xb0:'{degrees}',
		0xb1:'{+/-}', 0xb2:'{^2}', 0xb3:'{^3}', 0xb4:"'",
		0xb5:'{micro}', 0xb6:'{paragraph}', 0xb7:'*', 0xb8:'{cedilla}',
		0xb9:'{^1}', 0xba:'{^o}', 0xbb:'>>', 
		0xbc:'{1/4}', 0xbd:'{1/2}', 0xbe:'{3/4}', 0xbf:'?',
		0xd7:'*', 0xf7:'/'
		}
	r = ''
	for i in unicharacters:
		if xlate.has_key(ord(i)):
			r += xlate[ord(i)]
		elif ord(i) >= 0x80:
			pass
		else:
			r += str(i)
	return r

def shorten(url):
	apiUrl = 'http://bit.ly/api'
	values = {'url' : url,}
	data = urllib.urlencode(values)
	req = urllib2.Request(apiUrl, data)
	response = urllib2.urlopen(req)
	return response.read()

def blurb(text, length, addDots=True, debug=False):
	"""Reduces a text to length or less one word at a time, optionally adding..."""
	if debug:
		print "Blurb |%s|" % text
	if len(text) <= length or len(text) == 0:
		return text
	else:
		if len(text) > length:
			text = text[0:length]
		(t,u,v) = text.rpartition(' ')
		if addDots is True:
			return '%s...' % blurb(t, length-3, False, debug)
		else:
			return blurb(t, length, False, debug)

class Serializer(threading.Thread):
	def __init__(self, **kwds):
		super(Serializer, self).__init__(**kwds)
		self.setDaemon(1)
		self.setName('Serializer')
		self.workRequestQueue = Queue.Queue()
		self.resultQueue = Queue.Queue()
		self.start()
	
	def apply(self, callable, *args, **kwds):
		"""called by other threads as callable would be"""
		self.workRequestQueue.put((callable, args, kwds))
		return self.resultQueue.get()
	
	def run(self):
		while 1:
			callable, args, kwds = self.workRequestQueue.get()
			self.resultQueue.put(callable(*args, **kwds))
	

class rss2twitter():
	"""Takes a tuple of RSS feeds and twitter credentials and reads one, posts to the other."""
		
	debug = False
		
	def __init__(self, username, password, feeds=None, cacheDir = './', tag="New %s post"):
		self.timers = []
		self.twitQueue = Serializer()
		self.twitApi = twitter.Api()
		self.tag = tag
		self.feeds = feeds
		self.twitApi.SetCredentials(username, password)
		self.feedHistory = os.path.join(cacheDir, 'db')
		
		conn = sqlite3.connect(self.feedHistory)
		c = conn.cursor()
		c.execute('''create table if not exists users (username text primary key, title text)''')
		if feeds is not None:
			for f in feeds:
				if self.debug is True:
					print "creating table for %s" % f
				c.execute('create table if not exists "%s" (hash text primary key, date text)' % hashlib.sha1(f).hexdigest())
		conn.commit()
		c.close()
	
	def doDirectMessages(self, timerIndex):
		"""Process Direct Messages, queue posts"""
		self.timers[timerIndex]=threading.Timer(DIRECT_MESSAGE_DELAY, self.doDirectMessages, (timerIndex,))
		self.timers[timerIndex].setName('DirectMessageTimer')
		self.timers[timerIndex].start()
	
	def doRSSFeed(self, timerIndex, feedUrl):
		"""Process RSS Feed, queue posts for new items"""
		if self.debug is True:
			print "processing %s" % feedUrl
		feed = feedparser.parse(feedUrl)
		feedtitle = feed.feed.title
		for e in feed.entries:
			if self.wasPublished(hashlib.sha1(feedUrl).hexdigest(), e) is not True:
				if (self.tag == False or self.tag == ""):
					txt = ""
				else:
					if(self.tag.find('%') > -1):
						txt = "%s:" % self.tag % feedtitle
					else:
						txt = "%s:" % self.tag
				if (e.title is not ""):
					txt = "%s %s:" % (txt, e.title)
				link = shorten(e.link)
				txtr = "%s %s [%s]"
				txt  = (txtr % (txt, blurb(e.summary, POST_LENGTH - len((txtr % (txt, "", link)).strip()), debug=True), link)).strip()
				if self.debug:
					print "----\n%s" % txt
					print "Length %s" % len(txt)
				#self.postTweet(txt)
				#threading.Thread(target=self.postTweet, name="postTweet-%s"%hashlib.sha1(txt).hexdigest()[0:6], args=(txt,)).start()
				if self.debug is True:
					print "Queueing tweet %s in thread %s." % (hashlib.sha1(txt).hexdigest()[0:6], threading.currentThread().getName())
				self.twitQueue.apply(self.postTweet, txt)
		self.timers[timerIndex]=threading.Timer(RSS_FEED_DELAY, self.doRSSFeed, (timerIndex, feedUrl))
		self.timers[timerIndex].setName("RSSFeedTimer-%s" % timerIndex)
		self.timers[timerIndex].start()
	
	def checkDirectMessages(self):
		"""Check for new Direct Messages"""
		pass
	
	def postTweet(self, msgText):
		"""Post a Tweet"""
		posted = False
		while posted is not True:
			try:
				#self.twitQueue.apply(self.twitApi.PostUpdate, msgText)
				self.twitApi.PostUpdate(msgText)
			except urllib2.HTTPError, err:
				errno = int(err.info().items()[0][1][0:3])
				if self.debug:
					print "Error no: %s" % errno
				if errno == 401:
					if self.debug:
						print "Rate limited, sleeping for 5 mins"
					time.sleep(300)
				elif errno == 502:
					if self.debug:
						print "Server upgrade, sleeping for 15 mins"
					time.sleep(900)
				elif errno == 503:
					if self.debug:
						print "Server busy, sleeping for 30 mins"
					time.sleep(1800)
				else:
					raise twitter.TwitterError(err.info().items()[0][1])
			else:
				if self.debug is True:
					print "Tweet %s posted in thread %s." % (hashlib.sha1(msgText).hexdigest()[0:6], threading.currentThread().getName())
				posted = True
	
	def sendDirectMessage(self, msgText):
		"""Send a Direct Message"""
		pass
	
	def deleteDirectMessage(self, msgID):
		"""Delete a DirectMessage"""
		pass
	
	def run(self, doDirect=False, debug=False):
		"""Start processing everything"""
		self.debug = debug
		if doDirect==True:
			if self.debug is True:
				print "Creating first directmessage timer as timer %s" % len(self.timers)
			self.timers.append(threading.Timer(1, self.doDirectMessages, (len(self.timers),)))
		if self.feeds is not None:
			for f in self.feeds:
				if self.debug is True:
					print "Creating first rssfeed timer for %s as timer %s" % (f, len(self.timers),)
				self.timers.append(threading.Timer(1, self.doRSSFeed, (len(self.timers), f)))
		if self.debug is True:
			print "Creating first timestamp timer as timer %s" % len(self.timers)
			self.timers.append(threading.Timer(1, self.printTimestamp, (len(self.timers),)))
		for t in self.timers:
			if self.debug is True:
				print "Starting timer"
			t.start()
	
	def wasPublished(self, feedTable, feedEntry, storeHistory=True):
		"""Checks to see if a feed item has been previously published"""
		conn = sqlite3.connect(self.feedHistory)
		c = conn.cursor()
		feedEntry.summary = latin1_to_ascii(feedEntry.summary)
		entryVal = hashlib.sha1(feedEntry.summary).hexdigest()
		if self.debug is True:
			print "checking %s for published status" % feedEntry.title
		c.execute('select date from "%s" where hash=?' % feedTable, (entryVal,))
		if len(c.fetchall()) > 0:
			c.close()
			if self.debug is True:
				print "published"
			return True
		else:
			if self.debug is True:
				print "unpublished"
			if storeHistory is True:
				if self.debug is True:
					print "storing"
				t = (entryVal, time.time(),)
				c.execute('insert into "%s" values(?, ?)' % feedTable, t)
				conn.commit()
			c.close()
			return False
	
	def printTimestamp(self, timerIndex):
		"""pretty print's a timestamp, optionally the time specified"""
		print "Current Time: %s | Queue Size: %s" % (time.asctime(), self.twitQueue.workRequestQueue.qsize())
		self.timers[timerIndex]=threading.Timer(5, self.printTimestamp, (timerIndex,))
		self.timers[timerIndex].setName("TimestampTimer-%s" % timerIndex)
		self.timers[timerIndex].start()
