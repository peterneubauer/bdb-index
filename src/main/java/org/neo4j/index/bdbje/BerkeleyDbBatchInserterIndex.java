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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.cache.LruCache;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

class BerkeleyDbBatchInserterIndex implements BatchInserterIndex
{
    private final IndexIdentifier identifier;
    
    private final boolean createdNow;
    private Map<String, LruCache<String, Collection<Long>>> cache;

    private Map<String, Database> dbs = new HashMap<String, Database>();

    private Pair<String, Boolean> storeDir;

    BerkeleyDbBatchInserterIndex( BerkeleyDbBatchInserterIndexProvider provider,
            BatchInserter inserter, IndexIdentifier identifier, Map<String, String> config )
    {
        String dbStoreDir = ((BatchInserterImpl) inserter).getStore();
        storeDir = BerkeleyDbDataSource.getStoreDir( dbStoreDir + "/"+identifier.indexName );
        this.createdNow = storeDir.other();
        this.identifier = identifier;
    }

    public void add( long entityId, Map<String, Object> properties )
    {
        try
        {
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                String key = entry.getKey();
                Database db = dbs.get(key);
                if(null == db ) {
                    db = createDB(key);
                    dbs.put( key, db );
                }
                String value = entry.getValue().toString();
                DatabaseEntry valueEntry = new DatabaseEntry(value.getBytes("UTF-8"));
                ByteArrayOutputStream baus = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream( baus );
                dos.writeLong( entityId );
                dos.flush();
                db.put( null, valueEntry, new DatabaseEntry(baus.toByteArray()) );
                
                dos.close();
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
    
    private Database createDB( String key )
    {
        try
        {
            EnvironmentConfig environmentConfig = new EnvironmentConfig();
            environmentConfig.setAllowCreate( true );
            environmentConfig.setDurability( Durability.COMMIT_WRITE_NO_SYNC );
            environmentConfig.setCachePercent( 50 );
//            environmentConfig.setConfigParam( "java.util.logging.level", "INFO" );
            // perform other environment configurations
            String dir = BerkeleyDbDataSource.getStoreDir( this.storeDir.first() + "/" + key ).first();
            Environment environment = new Environment( new File(
                    dir ), environmentConfig );
            environmentConfig.setCacheSize(16000000 );//16MB
            environmentConfig.setTransactional( false );
            DatabaseConfig databaseConfig = new DatabaseConfig();
            databaseConfig.setAllowCreate( true );
            // perform other database configurations
            Database db = environment.openDatabase( null, key,
                    databaseConfig );
            return db;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    
    public void updateOrAdd( long entityId, Map<String, Object> properties )
    {
        try
        {
            add( entityId, properties );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }



    public IndexHits<Long> get( String key, Object value )
    {
        ArrayList<Long> resultList = new ArrayList<Long>();
        Database db = dbs.get( key );
        if(null == db) {
            return new IndexHitsImpl<Long>( resultList, 0 );
        }
        DatabaseEntry result = new DatabaseEntry();
        try
        {
            OperationStatus status = db.get( null, new DatabaseEntry(value.toString().getBytes("UTF-8")), result, LockMode.READ_UNCOMMITTED );
            if(status == OperationStatus.NOTFOUND) {
                return new IndexHitsImpl<Long>( resultList, 0 );
            }
            byte[] data = result.getData();
            if(null==data) {
                return new IndexHitsImpl<Long>( resultList, 0 );
            }
            DataInputStream dis = new DataInputStream( new ByteArrayInputStream( data ) );
            resultList.add( dis.readLong() );
        }
        catch ( Exception e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new IndexHitsImpl<Long>( resultList, resultList.size() );
    }


    public void shutdown()
    {
    }
    
    public void flush()
    {
        //writerModified = true;
    }

    @Override
    public IndexHits<Long> query( String key, Object queryOrQueryObject )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IndexHits<Long> query( Object queryOrQueryObject )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setCacheCapacity( String key, int size )
    {
        // TODO Auto-generated method stub
        
    }
    
}
