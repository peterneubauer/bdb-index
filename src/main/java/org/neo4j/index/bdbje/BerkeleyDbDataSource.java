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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.index.IndexProviderStore;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.transaction.xaframework.LogBackedXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransactionFactory;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

/**
 * An {@link XaDataSource} optimized for the {@link LuceneIndexProvider}. This
 * class is public because the XA framework requires it.
 */
public class BerkeleyDbDataSource extends LogBackedXaDataSource
{
    public static final String DEFAULT_NAME = "bdb";
    public static final byte[] DEFAULT_BRANCH_ID = "231564".getBytes();
    private static final String DB_NAME = "berkeleydb";

    private final XaContainer xaContainer;
    private final String baseStorePath;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    final IndexStore indexStore;
    final IndexProviderStore store;
    private boolean closed;

    private final Map<IndexIdentifier, Map<String, Database>> databases = new HashMap<IndexIdentifier, Map<String, Database>>();

    /**
     * Constructs this data source.
     * 
     * @param params XA parameters.
     * @throws InstantiationException if the data source couldn't be
     *             instantiated
     */
    public BerkeleyDbDataSource( Map<Object, Object> params )
                                                             throws InstantiationException
    {
        super( params );
        String storeDir = (String) params.get( "store_dir" );
        this.baseStorePath = getStoreDir( storeDir ).first();
        this.indexStore = (IndexStore) params.get( IndexStore.class );
        this.store = newIndexStore( storeDir );
        boolean isReadOnly = params.containsKey( "read_only" ) ? (Boolean) params.get( "read_only" )
                : false;

        if ( !isReadOnly )
        {
            XaCommandFactory cf = new BerkeleyDbCommandFactory();
            XaTransactionFactory tf = new BerkeleyDbTransactionFactory();
            xaContainer = XaContainer.create( this, this.baseStorePath
                                                    + "/logical.log", cf, tf,
                    params );
            try
            {
                xaContainer.openLogicalLog();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( "Unable to open bekeleydb log in "
                                            + this.baseStorePath, e );
            }

            xaContainer.getLogicalLog().setKeepLogs(
                    shouldKeepLog( (String) params.get( "keep_logical_logs" ),
                            DEFAULT_NAME ) );
            setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
        }
        else
        {
            xaContainer = null;
        }

    }

    static Pair<String, Boolean> getStoreDir( String dbStoreDir )
    {
        File dir = new File( dbStoreDir );
        boolean created = false;
        if ( !dir.exists() )
        {
            if ( !dir.mkdirs() )
            {
                throw new RuntimeException( "Unable to create directory path["
                                            + dir.getAbsolutePath()
                                            + "] for Neo4j store." );
            }
            created = true;
        }
        if ( dir.list().length == 0 )
        {
            created = true;
        }
        return Pair.of( dir.getAbsolutePath(), created );
    }

    static IndexProviderStore newIndexStore( String dbStoreDir )
    {
        return new IndexProviderStore( new File(
                getStoreDir( dbStoreDir ).first() + "/store.db" ) );
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            return;
        }
        // TODO
        store.close();
        try
        {
            // berkeleyDb.close();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        closed = true;
    }

    @Override
    public XaConnection getXaConnection()
    {
        return new BerkeleyDbXaConnection( baseStorePath,
                xaContainer.getResourceManager(), getBranchId() );
    }

    private class BerkeleyDbCommandFactory extends XaCommandFactory
    {
        BerkeleyDbCommandFactory()
        {
            super();
        }

        @Override
        public XaCommand readCommand( ReadableByteChannel channel,
                ByteBuffer buffer ) throws IOException
        {
            return BerkeleyDbCommand.readCommand( channel, buffer,
                    BerkeleyDbDataSource.this );
        }
    }

    private class BerkeleyDbTransactionFactory extends XaTransactionFactory
    {
        @Override
        public XaTransaction create( int identifier )
        {
            return createTransaction( identifier, this.getLogicalLog() );
        }

        @Override
        public void flushAll()
        {
            // Not much we can do...
        }

        @Override
        public long getCurrentVersion()
        {
            return store.getVersion();
        }

        @Override
        public long getAndSetNewVersion()
        {
            return store.incrementVersion();
        }

        @Override
        public long getLastCommittedTx()
        {
            return store.getLastCommittedTx();
        }
    }

    void getReadLock()
    {
        lock.readLock().lock();
    }

    void releaseReadLock()
    {
        lock.readLock().unlock();
    }

    void getWriteLock()
    {
        lock.writeLock().lock();
    }

    void releaseWriteLock()
    {
        lock.writeLock().unlock();
    }

    XaTransaction createTransaction( int identifier, XaLogicalLog logicalLog )
    {
        return new BerkeleydbTransaction( identifier, logicalLog, this );
    }

    @Override
    public long getCreationTime()
    {
        return store.getCreationTime();
    }

    @Override
    public long getRandomIdentifier()
    {
        return store.getRandomNumber();
    }

    @Override
    public long getCurrentLogVersion()
    {
        return store.getVersion();
    }

    public static byte[] indexKey( String key, Object value )
    {
        return String.valueOf( key + "|" + value ).getBytes();
    }

    public Database getDatabase( IndexIdentifier identifier, Object key )
    {
        Map<String, Database> db = databases.get( identifier );
        if ( null == db )
        {
            db = new HashMap<String, Database>();
            databases.put( identifier, db );
        }
        Database result = db.get( key.toString() );
        if ( null == result )
        {
            result = createDB( identifier, key );
            db.put( key.toString(), result );
        }

        return result;
    }

    public void addEntry( Database db, IndexIdentifier identifier,
            long[] entityIds, String key, String value )
    {
        byte[] indexKey = indexKey( key, value );
        long[] existingIds = getExistingIds( db, indexKey );
        long[] ids = ArrayUtil.include( existingIds, entityIds );
        try
        {
            db.put( null, new DatabaseEntry( indexKey ), new DatabaseEntry(
                    ArrayUtil.toBytes( ids ) ) );
        }
        catch ( DatabaseException e )
        {
            e.printStackTrace();
        }
    }

    private long[] getExistingIds( Database db, byte[] key )
    {
        try
        {
            DatabaseEntry value = new DatabaseEntry();
            db.get( null, new DatabaseEntry( key ), value,
                    LockMode.READ_UNCOMMITTED );

            return value.getData() != null ? ArrayUtil.toLongArray( value.getData() )
                    : new long[0];
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    public void removeEntry( Database db, IndexIdentifier identifier,
            long[] entityIds, String key, String value )
    {
        // byte[] indexKey = indexKey( key, value );
        // long[] existingIds = getExistingIds( db, indexKey );
        // long[] ids = ArrayUtil.exclude( existingIds, entityIds );
        // if ( ids.length == 0 )
        // {
        // batch.addDelete( 0, indexKey );
        // }
        // else
        // {
        // batch.addInsert( 0, indexKey, ArrayUtil.toBytes( ids ) );
        // }
    }

    public void commit( Database db )
    {
        // try
        // {
        // db.insert( null ).get();
        // }
        // catch ( BabuDBException e )
        // {
        // throw new RuntimeException( e );
        // }
    }

    public long getLastCommittedTxId()
    {
        return this.store.getLastCommittedTx();
    }

    private Database createDB( IndexIdentifier identifier, Object key )
    {
        try
        {
            EnvironmentConfig environmentConfig = new EnvironmentConfig();
            environmentConfig.setAllowCreate( true );
            // environmentConfig.setConfigParam( "java.util.logging.level",
            // "INFO" );
            // perform other environment configurations
            String dir = BerkeleyDbDataSource.getStoreDir(
                    this.baseStorePath + "/index/bdb/"
                            + identifier.itemClass.getSimpleName() + "/"
                            + identifier.indexName + "/" + key ).first();
            Environment environment = new Environment( new File( dir ),
                    environmentConfig );
            environmentConfig.setTransactional( false );
            DatabaseConfig databaseConfig = new DatabaseConfig();
            databaseConfig.setAllowCreate( true );
            // perform other database configurations
            Database db = environment.openDatabase( null, key.toString(),
                    databaseConfig );
            return db;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
