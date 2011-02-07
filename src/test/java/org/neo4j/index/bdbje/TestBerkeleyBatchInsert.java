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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

public class TestBerkeleyBatchInsert extends Neo4jTestCase
{
    private static final String PATH = "target/var/batch";
    private static final int MAX = 1000000;

    @Before
    public void cleanDirectory()
    {
        Neo4jTestCase.deleteFileOrDirectory( new File( PATH ) );
    }

    @Test
    public void testSome() throws Exception
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new BerkeleyDbBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "users",
                BerkeleyDbIndexProvider.DEFAULT_CONFIG );
        for ( int i = 0; i < MAX; i++ )
        {
            if ( i % 10000 == 0 )
            {
                // restartTx();
                System.out.println( i );
            }
            inserter.createNode( null );
            index.add( i, MapUtil.map( "name", "Joe" + i, "other", "Schmoe" ) );
            // ids.put( i, id );
        }

        for ( int i = 0; i < MAX; i++ )
        {
            if ( i % 10000 == 0 )
            {
                // restartTx();
                System.out.println( i );
            }
            int key = (int) Math.floor( Math.random() * MAX );
            assertEquals( key + "",
                    index.get( "name", "Joe" + key ).getSingle().toString() );
        }
        provider.shutdown();
        inserter.shutdown();

        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH );
        assertTrue( db.index().existsForNodes( "users" ) );
        Index<Node> dbIndex = db.index().forNodes( "users" );
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( i,
                    dbIndex.get( "name", "Joe" + i ).getSingle().getId() );
        }
        db.shutdown();
    }

    @Test
    public void testInsertionSpeed()
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider provider = new BerkeleyDbBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex index = provider.nodeIndex( "yeah",
                BerkeleyDbIndexProvider.DEFAULT_CONFIG );
        index.setCacheCapacity( "key", 1000000 );
        long t = System.currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            Map<String, Object> properties = MapUtil.map( "key", "value" + i );
            long id = inserter.createNode( properties );
            index.add( id, properties );
        }
        System.out.println( "insert:" + ( System.currentTimeMillis() - t ) );
        index.flush();

        t = System.currentTimeMillis();
        for ( int i = 0; i < 1000000; i++ )
        {
            IteratorUtil.count( (Iterator<Long>) index.get( "key", "value" + i ) );
        }
        System.out.println( "get:" + ( System.currentTimeMillis() - t ) );
    }

    @Test
    public void testFindCreatedIndex()
    {
        String indexName = "persons";
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BerkeleyDbBatchInserterIndexProvider indexProvider = new BerkeleyDbBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex persons = indexProvider.nodeIndex( "persons",
                BerkeleyDbIndexProvider.DEFAULT_CONFIG );
        Map<String, Object> properties = MapUtil.map( "name", "test" );
        long node = inserter.createNode( properties );
        persons.add( node, properties );
        indexProvider.shutdown();
        inserter.shutdown();
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( PATH );
        Transaction tx = graphDb.beginTx();
        try
        {
            IndexManager indexManager = graphDb.index();
            Assert.assertFalse( indexManager.existsForRelationships( indexName ) );
            Assert.assertTrue( indexManager.existsForNodes( indexName ) );
            Assert.assertNotNull( indexManager.forNodes( indexName ) );
            Index<Node> nodes = graphDb.index().forNodes( indexName );
            Assert.assertTrue( nodes.get( "name", "test" ).hasNext() );
            tx.success();
            tx.finish();
        }
        finally
        {
            graphDb.shutdown();
        }
    }

    @Ignore
    @Test
    public void testCanIndexRelationships()
    {
        BatchInserter inserter = new BatchInserterImpl( PATH );
        BatchInserterIndexProvider indexProvider = new BerkeleyDbBatchInserterIndexProvider(
                inserter );
        BatchInserterIndex edgesIndex = indexProvider.relationshipIndex(
                "edgeIndex",
                MapUtil.stringMap( "provider", "lucene", "type", "exact" ) );

        long nodeId1 = inserter.createNode( MapUtil.map( "ID", "1" ) );
        long nodeId2 = inserter.createNode( MapUtil.map( "ID", "2" ) );
        long relationshipId = inserter.createRelationship( nodeId1, nodeId2,
                EdgeType.KNOWS, null );

        edgesIndex.add( relationshipId,
                MapUtil.map( "EDGE_TYPE", EdgeType.KNOWS.name() ) );
        edgesIndex.flush();

        assertEquals(
                String.format( "Should return relationship id" ),
                new Long( relationshipId ),
                edgesIndex.query( "EDGE_TYPE", EdgeType.KNOWS.name() ).getSingle() );

        indexProvider.shutdown();
        inserter.shutdown();
    }

    private enum EdgeType implements RelationshipType
    {
        KNOWS
    }
}
