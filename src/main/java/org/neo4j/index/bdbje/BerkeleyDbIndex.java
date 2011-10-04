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
	public void add( T entity, String key, Object value ) {
		// getConnection().add( this, entity, key, value );
		
		// directly commit stuff, no TX caching
		Database db = service.dataSource().getDatabase( identifier, key );
		List<Long> ids = new ArrayList<Long>();
		if ( entity instanceof Node ) {
			ids.add( ( (Node)entity ).getId() );
			service.dataSource().addEntry( db, identifier, ArrayUtil.toPrimitiveLongArray( ids ), key, value );
		}
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
		List<Long> ids = new ArrayList<Long>();
		try
		
		{
			DatabaseEntry result = new DatabaseEntry();
			OperationStatus status =
					db.get( null, new DatabaseEntry( BerkeleyDbDataSource.indexKey( key, value ) ), result,
						LockMode.READ_UNCOMMITTED );
			byte[] bytes = result.getData();
			if ( bytes != null ) {
				for ( long id : ArrayUtil.toLongArray( bytes ) ) {
					ids.add( id );
				}
			}
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		} finally {
			service.dataSource().releaseReadLock();
		}
		
		Iterator<T> entities = new IteratorWrapper<T, Long>( ids.iterator() ) {
			
			@Override
			protected T underlyingObjectToObject( Long id ) {
				return idToEntity( id );
			}
		};
		return new IndexHitsImpl<T>( entities, ids.size() );
	}
	
	
	protected abstract T idToEntity( Long id );
	
	
	@Override
	public IndexHits<T> query( Object queryOrQueryObject ) {
		throw new UnsupportedOperationException();
	}
	
	
	@Override
	public IndexHits<T> query( String key, Object queryOrQueryObject ) {
		throw new UnsupportedOperationException();
	}
	
	
	public void remove( Object queryOrQueryObject ) {
		throw new UnsupportedOperationException();
	}
	
	
	public void remove( T entity, Object queryOrQueryObjectOrNull ) {
		throw new UnsupportedOperationException();
	}
	
	
	@Override
	public void remove( T entity, String key, Object value ) {
		getConnection().remove( this, entity, key, value );
	}
	
	
	@Override
	public void delete() {
		System.err.println( "bdb index delete" );
		for ( Map<String, Database> dbs : service.dataSource().getDatabases().values() ) {
			for ( Database db : dbs.values() ) {
				if ( db.getEnvironment().isValid() ) {
					System.err.println( "bdb environ closing:" + db.getEnvironment().getHome() );
					db.close();
					db.getEnvironment().close();
				}
			}
		}
	}
	
	static class NodeIndex extends BerkeleyDbIndex<Node> {
		
		NodeIndex( BerkeleyDbIndexImplementation implementation, IndexIdentifier identifier ) {
			super( implementation, identifier );
		}
		
		
		@Override
		protected Node idToEntity( Long id ) {
			return service.graphDb().getNodeById( id );
		}
		
		
		@Override
		public String getName() {
			return "bdb-relationships";
		}
		
		
		@Override
		public Class<Node> getEntityType() {
			return Node.class;
		}
		
		
		@Override
		public void remove( Node arg0 ) {
			// TODO Auto-generated method stub
			
		}
		
		
		@Override
		public void remove( Node arg0, String arg1 ) {
			// TODO Auto-generated method stub
			
		}
		
		
		@Override
		public boolean isWriteable() {
			// TODO Auto-generated method stub
			return false;
		}
	}
	
	static class RelationshipIndex extends BerkeleyDbIndex<Relationship> {
		
		RelationshipIndex( BerkeleyDbIndexImplementation implementation, IndexIdentifier identifier ) {
			super( implementation, identifier );
		}
		
		
		@Override
		protected Relationship idToEntity( Long id ) {
			return service.graphDb().getRelationshipById( id );
		}
		
		
		@Override
		public String getName() {
			return "bdb-relationships";
		}
		
		
		@Override
		public Class<Relationship> getEntityType() {
			return Relationship.class;
		}
		
		
		@Override
		public void remove( Relationship arg0 ) {
			// TODO Auto-generated method stub
			
		}
		
		
		@Override
		public void remove( Relationship arg0, String arg1 ) {
			// TODO Auto-generated method stub
			
		}
		
		
		@Override
		public boolean isWriteable() {
			// TODO Auto-generated method stub
			return false;
		}
	}
}
