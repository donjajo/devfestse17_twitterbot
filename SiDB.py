#!/usr/bin/python3
from sqlite3 import Connection

class SiDB( Connection ):
	def __init__( self, database ):
		self.database = database
		super( self.__class__, self ).__init__( self.database )
		self.cursor = self.cursor()

	def insert( self, table, data ):
		"""Inserts data in the table and returns ID of the last inserted ID
		Accepts table name and data 
		data passed must be dict having the column name as key and value"""

		#Make sure its dictionary passed
		if not isinstance( data, dict ):
			raise ValueError( 'Dictionary data type needed in %s' % self.__class__.__name__ + '.' + self.insert.__name__ )

		#Get columns and join with commas (,)
		columns = list( data.keys() )
		columns = ', '.join( columns )

		#Get values
		values = list( data.values() )
		#Generate ? placeholders based on the length of the values
		placeholder = ', '.join( [ '?' for x in range( 0, len( values ) ) ] )

		#Finally insert into database
		sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ( table, columns, placeholder )
		self.cursor.execute( sql, values )
		self.commit()

		#Return last inserted row
		return self.cursor.lastrowid

	def __create_query( self, data, statement = '`{key}` = {value}', placeholder_prefix = 'data' ):
		"""Private method to easily create queries"""
		#Create new unique placeholders
		placeholders = [ ':' + str( placeholder_prefix ) + str( x ) for x in range( 0, len( data )  ) ]
		response = dict()
		response[ 'query' ] = []

		merge = list( zip( list( data.keys() ), placeholders ) )
		for i in dict( merge ).items():
			state = statement
			state = state.replace( '{key}', i[ 0 ] )
			state = state.replace( '{value}', i[ 1 ] )
			response[ 'query' ].append( state )

		#Join values with placeholders
		placeholders = [ x[ 1: ] for x in placeholders ]
		response[ 'data' ] = zip( placeholders, list( data.values() ) )
		response[ 'data' ] = dict( list(  response[ 'data' ] ) )
		return response

	def update( self, table, data, where = {} ):
		"""Updates a table 
		data parameter must be dict having the column name as key and value as value 
		where parameter is also a dict type having column name as key and value as value
		"""
		#To make sure its dict passed
		if not isinstance( data, dict ):
			raise ValueError( 'Dict datatype needed in update method' )

		#Generate queries and parameters needed
		set_query = self.__create_query( data )
		placeholders = set_query[ 'data' ]

		#Prepare SQL
		sql = "UPDATE %s SET %s " % ( table, ', '.join( set_query[ 'query' ] ) )

		#Merge WHERE statement if specified
		if where:
			where_query = self.__create_query( where, placeholder_prefix = 'where' )
			sql = sql + " WHERE %s " % ' AND '.join( where_query[ 'query' ] )
			placeholders.update( where_query[ 'data' ] )
		self.cursor.execute( sql, placeholders )
		self.commit()

	def delete( self, table, where = {} ):
		"""Deletes row(s) from a table 
		where parameter, if passed, must be dict. Column name as key and value as value 
		"""
		# To make sure it's dict
		if not isinstance( where, dict ):
			raise ValueError( 'Dict data type needed in delete method' )

		placeholders = {}

		sql = "DELETE FROM %s " % table 
		if where:
			where_query = self.__create_query( where, placeholder_prefix = 'where' )
			sql = sql + " WHERE %s " % ' AND '.join( where_query[ 'query' ] )
			placeholders.update( where_query[ 'data' ] )
		self.cursor.execute( sql, placeholders )
		self.commit()

	def table_exists( self, table ):
		"""Checks if a table exists in a database
		Returns boolean 
		"""
		self.cursor.execute( "SELECT NULL FROM `sqlite_master` WHERE `name` = :table AND type = 'table'", { 'table' : table } )
		self.commit()
		return True if self.cursor.fetchone() else False 

	def drop_table( self, table ): 
		"""Drops a table in a database"""
		self.cursor.execute( "DROP TABLE IF EXISTS %s" % table )
		self.commit()

	def create_table( self, table, data = {} ):
		"""This method is use for creating table, keys for a column allowed are
		* type - This specifies the data type of a column
		* allow_null - Boolean, allow null data on column,
		* primary_key - Boolean 
		* unique - boolean
		"""	
		c = list()
		for column in data.items():
			t = 'TEXT' if 'type' not in column[ 1 ] or not column[ 1 ][ 'type' ] else column[ 1 ][ 'type' ]
			pk = 'PRIMARY KEY' if 'primary_key' in column[ 1 ] and column[ 1 ][ 'primary_key' ] else ''
			nn = 'NOT NULL' if 'allow_null' not in column[ 1 ] or not column[ 1 ][ 'allow_null' ] else ''
			u = 'UNIQUE' if 'unique' in column[ 1 ] and column[ 1 ][ 'unique' ] else ''
			default = 'DEFAULT ' + str( column[ 1 ][ 'default' ] ) if 'default' in column[ 1 ] and column[ 1 ][ 'default' ] else ''
			c.append( """
				`%s` %s %s %s %s %s""" % ( column[ 0 ], t, pk, nn, u, default ) )
		sql = """CREATE TABLE IF NOT EXISTS `%s`
			(
				%s
			)""" % ( table, ','.join( c ) )

		self.cursor.execute( sql )
		self.commit()
		return sql
			
	def rename_table( self, table, new_name ):
		"""Renames a Table 
		Returns Boolean"""
		try:
			self.cursor.execute( "ALTER TABLE `{0}` RENAME TO `{1}`".format( table, new_name ) )
			self.commit()
			return True;
		except:
			return False;

	def __del__( self ):
		self.close()