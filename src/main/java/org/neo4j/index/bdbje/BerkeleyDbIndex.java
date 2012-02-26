/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.bdbje;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.persist.EntityStore;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;



public abstract class BerkeleyDbIndex<T extends PropertyContainer> implements Index<T> {

	final BerkeleyDbIndexImplementation	service;
	final IndexIdentifier				identifier;


	BerkeleyDbIndex( BerkeleyDbIndexImplementation implementation, IndexIdentifier identifier ) {
		this.service = implementation;
		this.identifier = identifier;
	}


	BerkeleyDbXaConnection getConnection() {
		if ( service.broker() == null ) {
			throw new ReadOnlyDbException();
		}
		return service.broker().acquireResourceConnection();
	}

	BerkeleyDbXaConnection getReadOnlyConnection() {
		return service.broker() == null ? null : service.broker().acquireReadOnlyResourceConnection();
	}


	@Override
	public GraphDatabaseService getGraphDatabase() {
		return service.graphDb();
	}

	@Override
	public void add( T entity, String key, Object value ) {
		//getConnection().add( this, entity, key, value );

		// directly commit stuff, no TX caching
		Database db = service.dataSource().getDatabase( identifier, key );
		List<Long> ids = new ArrayList<Long>();
		ids.add(getEntityId(entity));
		service.dataSource().addEntry( db, identifier, ArrayUtil.toPrimitiveLongArray( ids ), key, value );
	}

	@Override
	public IndexHits<T> get( String key, Object value ) {
		// BerkeleyDbXaConnection connection = getReadOnlyConnection();
		// BerkeleydbTransaction tx = connection != null ? connection.getTx() : null;
		// Collection<Long> added = tx != null ? tx.getAddedIds( this, key, value ) :
		// Collections.<Long>emptyList();
		// Collection<Long> removed = tx != null ? tx.getRemovedIds( this, key, value ) :
		// Collections.<Long>emptyList();

		service.dataSource().getReadLock();
		Database db = service.dataSource().getDatabase( identifier, key );

		try {
			DatabaseEntry result = new DatabaseEntry();
			OperationStatus status =
					db.get( null,
							new DatabaseEntry( BerkeleyDbDataSource.indexKey( key, value ) ),
							result,
							LockMode.READ_UNCOMMITTED );

			if (status != OperationStatus.SUCCESS) {
				throw new RuntimeException("Data query got status " + status);
			}

			return new LightIndexHits(result.getData(), +1);

		} catch ( RuntimeException e ) {
			throw e;
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		} finally {
			service.dataSource().releaseReadLock();
		}
	}

	protected abstract T idToEntity( long id );
	protected abstract long getEntityId( T entity );


	@Override
	public T putIfAbsent(T entity, String key, Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public IndexHits<T> query( Object queryOrQueryObject ) {
		throw new UnsupportedOperationException();
	}


	@Override
	public IndexHits<T> query( String key, Object queryOrQueryObject ) {
		if (queryOrQueryObject instanceof DecreaseOrderQuery) {
			DecreaseOrderQuery query = (DecreaseOrderQuery)queryOrQueryObject;

			service.dataSource().getReadLock();
			Database db = service.dataSource().getDatabase( identifier, key );

			try {
				DatabaseEntry result = new DatabaseEntry();
				OperationStatus status =
						db.get( null,
								new DatabaseEntry( BerkeleyDbDataSource.indexKey( key, query._value ) ),
								result,
								LockMode.READ_UNCOMMITTED );

				if (status != OperationStatus.SUCCESS) {
					throw new RuntimeException("Data query got status " + status);
				}

				return new LightIndexHits(result.getData(), -1);
			} catch ( RuntimeException e ) {
				throw e;
			} catch ( Exception e ) {
				throw new RuntimeException( e );
			} finally {
				service.dataSource().releaseReadLock();
			}
		}
		throw new RuntimeException( "Unsuporded query "+queryOrQueryObject.getClass() );
	}


	@Override
	public void remove( T entity ) {
		throw new UnsupportedOperationException();

		//		for (Database db : service.dataSource().getDatabases().get(identifier).values()) {
		//			List<Long> ids = new ArrayList<Long>();
		//			ids.add(getEntityId(entity));
		//			service.dataSource().addEntry( db, identifier, ArrayUtil.toPrimitiveLongArray( ids ), key, value );
		//		}
	}


	@Override
	public void remove( T entity, String key ) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void remove( T entity, String key, Object value ) {
		getConnection().remove( this, entity, key, value );
	}


	@Override
	public void delete() {
		System.err.println("bdb index delete");
		for ( Map<String, Database> dbs : service.dataSource().getDatabases().values() ) {
			for ( Database db : dbs.values() ) {
				if ( db.getEnvironment().isValid() ) {
					System.err.println( "bdb environ closing:" + db.getEnvironment().getHome() );
					db.close();
					db.getEnvironment().close();
				}
			}
		}
		for ( Map<String, EntityStore> strs : service.dataSource().getEntityStores().values() ) {
			for ( EntityStore str : strs.values() ) {
				if ( str.getEnvironment().isValid() ) {
					System.err.println( "bdb environ closing:" + str.getEnvironment().getHome() );
					str.close();
					str.getEnvironment().close();
				}
			}
		}
	}

	@Override
	public boolean isWriteable() {
		return false;
	}


	static class NodeIndex extends BerkeleyDbIndex<Node> {

		NodeIndex( BerkeleyDbIndexImplementation implementation, IndexIdentifier identifier ) {
			super( implementation, identifier );
		}


		@Override
		protected Node idToEntity( long id ) {
			return service.graphDb().getNodeById( id );
		}

		@Override
		protected long getEntityId(Node entity) {
			return entity.getId();
		}


		@Override
		public String getName() {
			return "bdb-nodes";
		}


		@Override
		public Class<Node> getEntityType() {
			return Node.class;
		}

	}

	static class RelationshipIndex extends BerkeleyDbIndex<Relationship> implements org.neo4j.graphdb.index.RelationshipIndex {

		RelationshipIndex( BerkeleyDbIndexImplementation implementation, IndexIdentifier identifier ) {
			super( implementation, identifier );
		}

		@Override
		public IndexHits<Relationship> query(String s, Object o, Node start, Node end) {
			throw new UnsupportedOperationException();
		}

		@Override
		public IndexHits<Relationship> query(Object o, Node start, Node end) {
			throw new UnsupportedOperationException();
		}

		@Override
		public IndexHits<Relationship> get(String s, Object o, Node start, Node end) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected Relationship idToEntity( long id ) {
			return service.graphDb().getRelationshipById( id );
		}

		@Override
		protected long getEntityId(Relationship entity) {
			return entity.getId();
		}


		@Override
		public String getName() {
			return "bdb-relationships";
		}


		@Override
		public Class<Relationship> getEntityType() {
			return Relationship.class;
		}

	}

	class LightIndexHits implements IndexHits<T> {

		byte[] _array;
		int length;
		int pos = -1;
		int v;

		public LightIndexHits(byte[] array, int vector) {
			_array = array;
			v = vector;
			length = _array.length/8;

			if (v < 0) {
				pos = length;
			}
		}

		@Override
		public boolean hasNext() {
			if (v < 0) {
				return pos > 0;
			} else {
				return pos < length - 1;
			}
		}

		@Override
		public T next() {
			return idToEntity(
					ArrayUtil.toLong( _array, (pos += v)*8 )
					);
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Iterator<T> iterator() {
			return this;
		}

		@Override
		public int size() {
			return length;
		}

		@Override
		public void close() {
			//nothing to do
		}

		@Override
		public T getSingle() {
			if (length == 1 && pos == 0) {
				return next();
			}
			throw new NoSuchElementException();
		}

		@Override
		public float currentScore() {
			return 0;
		}

	}

}
