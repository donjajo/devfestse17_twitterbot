#!/usr/bin/env python3
import tweepy
import os
import sys
from SiDB import SiDB
from glob import glob 
from PIL import Image

class Tweet:
	images_dir = os.path.abspath( 'images' )
	tweeted_dir = os.path.abspath( 'tweeted' )

	details = {
		'consumer_key' : '',
		'consumer_secret' : '',
		'access_token' : '',
		'access_token_secret' : ''
	}

	def __init__( self ):
		self._db_connect()
		self._auth()

		self.tweet = ''

		# Define hashtags to be appended to the tweet
		self.hashtags = '#Devfest17 #DevfestSE17'

		# Create necessary paths
		if not os.path.exists( self.images_dir ):
			os.mkdir( self.images_dir )
		if not os.path.exists( self.tweeted_dir ):
			os.mkdir( self.tweeted_dir )

	def _db_connect( self ):
		"""Managing tweeted images with SiDB SQLite (https://github.com/donjajo/Python-SiDB)"""

		# Open or create database file if not existing
		self.db = SiDB( './images.db' )

		# If table does not exist, create it
		if not self.db.table_exists( 'tweeted' ):
			self.db.create_table( 'tweeted', {
				'image_name' : {
					'type' : 'LONGTEXT',
					'allow_null' : False,
					'primary_key' : True
				},
				'date' : {
					'type' : 'DATETIME',
					'default' : 'CURRENT_TIMESTAMP'
				}
			});

	def _auth( self ):
		"""Do all authentication to Twitter with Tweepy"""
		try:
			self.auth = tweepy.OAuthHandler( self.details[ 'consumer_key' ], self.details[ 'consumer_secret' ] );
			self.auth.set_access_token( self.details[ 'access_token' ], self.details[ 'access_token_secret' ] );

			self.api = tweepy.API( self.auth )
		except tweepy.error.TweepError as e:
			print( 'Error {0}: {1}'.format( e.args[ 0 ][ 0 ][ 'code' ], e.args[ 0 ][ 0 ][ 'message' ] ) )
			sys.exit( 1 )

	def comot_tweet( self, data ):
		""" Get Tweet from image file name"""
		tweet = os.path.splitext( data )[ 0 ]
		self.tweet = '{0}\n{1}'.format( tweet, self.hashtags )
		return self

	def put_am_watermark( self, img ):
		""" Add specified watermark"""
		watermark = Image.open( 'devfest.png' )
		watermarked = os.path.join( self.tweeted_dir, os.path.basename( img ) )
		image = Image.open( img )

		# Paste in the watermark image into the main image and set position. 
		# P.S the math there is hack i figured out to keep the watermark in place. It's working, don't change anything! :D
		image.paste( watermark, ( 0, image.size[ 1 ] - 350 ), watermark )

		# Save the watermarked image to tweeted directory
		image.save( watermarked )

		self.watermarked = watermarked

	def eh_don_tweet( self ):
		""" Just to check if tweet has been made before"""
		self.db.cursor.execute( "SELECT COUNT( `image_name` ) FROM `tweeted` WHERE `image_name` = ?", ( self.tweet, ) )
		return bool( self.db.cursor.fetchone()[ 0 ] )

	def _oya_tweet_am( self ):
		"""Finally, tweet it!"""

		# Check if tweet is loaded and watermark added too
		if self.tweet and self.watermarked:
			# Boom!
			self.api.update_with_media( self.watermarked, self.tweet )

			# SQLite boom
			self.db.insert( 'tweeted', {
				'image_name' : self.tweet
				})
			
			print( 'Tweeted: {0}'.format( self.tweet ) )

			# Oga, we are done. Give a factory reset slap
			self.watermarked = ''
			self.tweet = ''
			return True
		else:
			return False

	def do_am( self ):
		images = glob( '{0}/*.jpg'.format( self.images_dir ) )
		for image in images:
			self.comot_tweet( os.path.basename( image ) )
			if not self.eh_don_tweet():
				self.put_am_watermark( image )
				self._oya_tweet_am()

g = Tweet()
g.do_am()