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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;



public class TestBerkeley extends Neo4jTestCase {

	// @Override
	// protected boolean manageMyOwnTxFinish() {
	// return true;
	// }


	@Test
	public void testNode() throws Exception {
		Index<Node> index = graphDb().index().forNodes( "fastN", BerkeleyDbIndexImplementation.DEFAULT_CONFIG );
		// try {
		Node node1 = graphDb().createNode();
		Node node2 = graphDb().createNode();
		index.add( node1, "name", "Mattias" );
		index.add( node1, "node_osm_id", Integer.valueOf(123) );
		assertContains( index.get( "name", "Mattias" ), node1 );
		assertContains( index.get( "node_osm_id", Integer.valueOf(123) ), node1 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), node1 );
		assertContains( index.get( "node_osm_id", Integer.valueOf(123) ), node1 );
		index.add( node2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), node1, node2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), node1, node2 );

		index.remove( node1, "name", "Mattias" );
		// this should be better implemented
		assertContains( index.get( "name", "Mattias" ), node1, node2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), node2 );
		index.remove( node2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), node2 );
		node1.delete();
		node2.delete();

		// success();
		// } finally {
		// finish();
		// index.delete();
		// }
	}

	@Test
	public void testRelationship() throws Exception {
		Index<Relationship> index = graphDb().index().forRelationships( "fastR", BerkeleyDbIndexImplementation.DEFAULT_CONFIG );
		// try {
		RelationshipType rType = new RelationshipTypeImpl("test");

		Node node1 = graphDb().createNode();
		Node node2 = graphDb().createNode();
		Relationship r1 = node1.createRelationshipTo(node2, rType);
		index.add( r1, "name", "Mattias" );
		//TC have problems on this
		//index.add( r1, "r_osm_id", Integer.valueOf(123) );
		assertContains( index.get( "name", "Mattias" ), r1 );
		//TC have problems on this ... analyze
		//assertContains( index.get( "r_osm_id", Integer.valueOf(123) ), r1 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r1 );
		//TC have problems on this ... analyze
		//assertContains( index.get( "r_osm_id", Integer.valueOf(123) ), r1 );

		Relationship r2 = node1.createRelationshipTo(node2, rType);
		index.add( r2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), r1, r2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r1, r2 );

		assertContains( index.get( "name", "Mattias" ), r1, r2 );


		index.remove( r1, "name", "Mattias" );
		// this should be better implemented
		assertContains( index.get( "name", "Mattias" ), r1, r2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r2 );
		index.remove( r2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), r2 );
		r2.delete();
		r1.delete();
		node1.delete();
		node2.delete();

		// success();
		// } finally {
		// finish();
		// index.delete();
		// }
	}

	@Test
	@Ignore
	public void fullRelationshipIndex() throws Exception {
		Map<String, String> config = new HashMap<String, String>(BerkeleyDbIndexImplementation.DEFAULT_CONFIG);
		config.put("FullIndex", "true");
		Index<Relationship> index = graphDb().index().forRelationships( "fastR", config );

		Assert.assertEquals(RelationshipIndexFullImpl.class.getName(), index.getClass().getName());

		// try {
		RelationshipType rType = new RelationshipTypeImpl("test");

		Node node1 = graphDb().createNode();
		Node node2 = graphDb().createNode();
		Relationship r1 = node1.createRelationshipTo(node2, rType);
		index.add( r1, "name", "Mattias" );
		index.add( r1, "r_osm_id", Integer.valueOf(123) );
		assertContains( index.get( "name", "Mattias" ), r1 );
		assertContains( index.get( "r_osm_id", Integer.valueOf(123) ), r1 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r1 );
		assertContains( index.get( "r_osm_id", Integer.valueOf(123) ), r1 );

		Relationship r2 = node1.createRelationshipTo(node2, rType);
		index.add( r2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), r1, r2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r1, r2 );

		assertContains( index.get( "name", "Mattias" ), r1, r2 );


		index.remove( r1, "name", "Mattias" );
		// this should be better implemented
		assertContains( index.get( "name", "Mattias" ), r1, r2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r2 );
		index.remove( r2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), r2 );
		r2.delete();
		r1.delete();
		node1.delete();
		node2.delete();

		// success();
		// } finally {
		// finish();
		// index.delete();
		// }
	}

	@Test
	public void testQueryRelationship() throws Exception {
		Index<Relationship> index = graphDb().index().forRelationships( "fastR", BerkeleyDbIndexImplementation.DEFAULT_CONFIG );

		RelationshipType rType = new RelationshipTypeImpl("test");

		Node node1 = graphDb().createNode();
		Node node2 = graphDb().createNode();
		Relationship r1 = node1.createRelationshipTo(node2, rType);
		index.add( r1, "name", "Mattias" );

		Relationship r2 = node1.createRelationshipTo(node2, rType);
		index.add( r2, "name", "Mattias" );
		restartTx();

		assertContains( index.get( "name", "Mattias" ), r1, r2 );

		assertContains( index.query("name", new Query("Mattias")), r2, r1);

		r2.delete();
		r1.delete();
		node1.delete();
		node2.delete();
	}

	@Test
	public void testRelationshipQuery() throws Exception {
		RelationshipIndex index = graphDb().index().forRelationships( "fastR", BerkeleyDbIndexImplementation.DEFAULT_CONFIG );
		// try {
		RelationshipType rType = new RelationshipTypeImpl("test");

		Node node1 = graphDb().createNode();
		Node node2 = graphDb().createNode();
		Relationship r1 = node1.createRelationshipTo(node2, rType);
		index.add( r1, "name", "Mattias" );
		index.add( r1, "r_osm_id", Integer.valueOf(123) );
		assertContains( index.get("name", "Mattias"), r1 );
		assertContains( index.get("name", "Mattias"), r1 );
		assertContains( index.get("name", "Mattias"), r1 );
		assertContains( index.get( "r_osm_id", Integer.valueOf(123) ), r1 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r1 );
		assertContains( index.get( "r_osm_id", Integer.valueOf(123) ), r1 );

		Relationship r2 = node1.createRelationshipTo(node2, rType);
		index.add( r2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), r1, r2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r1, r2 );

		assertContains( index.get( "name", "Mattias" ), r1, r2 );


		index.remove( r1, "name", "Mattias" );
		// this should be better implemented
		assertContains( index.get( "name", "Mattias" ), r1, r2 );
		restartTx();
		assertContains( index.get( "name", "Mattias" ), r2 );
		index.remove( r2, "name", "Mattias" );
		assertContains( index.get( "name", "Mattias" ), r2 );
		r2.delete();
		r1.delete();
		node1.delete();
		node2.delete();

		// success();
		// } finally {
		// finish();
		// index.delete();
		// }
	}

	@Test
	public void testInsertSome() {
		Index<Node> index = graphDb().index().forNodes( "fast", MapUtil.stringMap( "provider", "berkeleydb-je" ) );
		// try {
		for ( int i = 0; i < 10000; i++ ) {
			Node node = graphDb().createNode();
			index.add( node, "yeah", "some long value " + ( i % 500 ) );
		}
		restartTx( true );

		for ( int i = 0; i < 500; i++ ) {
			IndexHits<Node> hits = index.get( "yeah", "some long value " + i );
			assertEquals( 20, hits.size() );
		}
		// success();
		// } finally {
		// finish();
		// index.delete();
		// }
	}


	@Ignore
	@Test
	public void testInsertionSpeed() {
		Index<Node> index = graphDb().index().forNodes( "speed", MapUtil.stringMap( "provider", "berkeleydb-je" ) );
		long t = System.currentTimeMillis();
		for ( int i = 0; i < 1000000; i++ ) {
			Node entity = graphDb().createNode();
			index.get( "name", "The name " + i );
			index.add( entity, "name", "The name " + i );
			index.add( entity, "title", "Some title " + i );
			index.add( entity, "something", i + "Nothing" );
			index.add( entity, "else", i + "kdfjkdjf" + i );
			if ( i % 10000 == 0 ) {
				// restartTx();
				System.out.println( i );
			}
		}
		System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );

		t = System.currentTimeMillis();
		int count = 1000;
		int resultCount = 0;
		for ( int i = 0; i < count; i++ ) {
			for ( Node entity : index.get( "name", "The name " + i * 900 ) ) {
				resultCount++;
			}
		}
		System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );

		t = System.currentTimeMillis();
		resultCount = 0;
		for ( int i = 0; i < count; i++ ) {
			for ( Node entity : index.get( "something", i * 900 + "Nothing" ) ) {
				resultCount++;
			}
		}
		System.out.println( "get(" + resultCount + "):" + (double)( System.currentTimeMillis() - t ) / (double)count );
	}

	private static class RelationshipTypeImpl implements RelationshipType {

		private final String name;

		RelationshipTypeImpl( String name ) {
			this.name = name;
		}

		@Override
		public String name() {
			return name;
		}
	}
}