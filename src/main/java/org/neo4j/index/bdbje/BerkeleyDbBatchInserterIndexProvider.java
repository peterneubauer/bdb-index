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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.LuceneIndexProvider;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.index.IndexStore;



/**
 * The {@link BatchInserter} version of {@link LuceneIndexProvider}. Indexes
 * created and populated using {@link BatchInserterIndex}s from this provider
 * are compatible with {@link Index}s from {@link LuceneIndexProvider}.
 */
public class BerkeleyDbBatchInserterIndexProvider implements BatchInserterIndexProvider {
	
	private final BatchInserter											inserter;
	private final Map<IndexIdentifier, BerkeleyDbBatchInserterIndex>	indexes	=
			new HashMap<IndexIdentifier, BerkeleyDbBatchInserterIndex>();
	final IndexStore													indexStore;
	
	
	public BerkeleyDbBatchInserterIndexProvider( final BatchInserter inserter ) {
		this.inserter = inserter;
		indexStore = ( (BatchInserterImpl)inserter ).getIndexStore();
		
	}
	
	
	@Override
	public BatchInserterIndex nodeIndex( String indexName, Map<String, String> config ) {
		config( Node.class, indexName, config );
		return index( new IndexIdentifier( Node.class, indexName ), config );
	}
	
	
	private Map<String, String> config( Class<? extends PropertyContainer> cls, String indexName, Map<String, String> config ) {
		// TODO Doesn't look right
		if ( config != null ) {
			config =
					MapUtil.stringMap( new HashMap<String, String>( config ), "provider",
						BerkeleyDbIndexImplementation.SERVICE_NAME );
			indexStore.setIfNecessary( cls, indexName, config );
			return config;
		} else {
			return indexStore.get( cls, indexName );
		}
	}
	
	
	@Override
	public BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config ) {
		config( Relationship.class, indexName, config );
		return index( new IndexIdentifier( Relationship.class, indexName ), config );
	}
	
	
	private BatchInserterIndex index( IndexIdentifier identifier, Map<String, String> config ) {
		// We don't care about threads here... c'mon... it's a
		// single-threaded batch inserter
		BerkeleyDbBatchInserterIndex index = indexes.get( identifier );
		if ( index == null ) {
			index = new BerkeleyDbBatchInserterIndex( this, inserter, identifier, config );
			indexes.put( identifier, index );
		}
		return index;
	}
	
	
	@Override
	public void shutdown() {
		System.err.println( "org.neo4j.index.bdbje.BerkeleyDbBatchInserterIndexProvider.shutdown()" );
		for ( BerkeleyDbBatchInserterIndex index : indexes.values() ) {
			index.shutdown();
		}
	}
}
