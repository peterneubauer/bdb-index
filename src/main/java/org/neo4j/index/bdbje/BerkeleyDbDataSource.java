/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.bdbje;

import com.sleepycat.je.*;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.impl.index.IndexProviderStore;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.transaction.xaframework.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An {@link XaDataSource} optimized for the {@link BerkeleyDbIndexProvider}. This
 * class is public because the XA framework requires it.
 */
public class BerkeleyDbDataSource extends LogBackedXaDataSource {

	public static final String									DEFAULT_NAME		= "bdb";
	public static final byte[]									DEFAULT_BRANCH_ID	= UTF8.encode( "231564" );

	private final XaContainer									xaContainer;
	private final String										baseStorePath;
	private final ReentrantReadWriteLock						lock				= new ReentrantReadWriteLock();
	final IndexStore											indexStore;
	final IndexProviderStore									store;
	private boolean												closed;

	private final boolean isReadOnly;

	private final Map<IndexIdentifier, Map<String, Database>>	databases			=
			new HashMap<IndexIdentifier, Map<String, Database>>();

	private final Map<IndexIdentifier, Map<String, EntityStore>>entityStores		=
			new HashMap<IndexIdentifier, Map<String, EntityStore>>();


	/**
	 * Constructs this data source.
	 * 
	 * @param params
	 *            XA parameters.
	 * @throws InstantiationException
	 *             if the data source couldn't be
	 *             instantiated
	 */
	public BerkeleyDbDataSource( Map<Object, Object> params ) throws InstantiationException {
		super( params );
		String storeDir = (String)params.get( "store_dir" );
		baseStorePath = getStoreDir( storeDir ).first();
		indexStore = (IndexStore)params.get( IndexStore.class );
		store = newIndexStore( storeDir );
		isReadOnly = params.containsKey( "read_only" ) ? (Boolean)params.get( "read_only" ) : false;

		if ( !isReadOnly ) {
			XaCommandFactory cf = new BerkeleyDbCommandFactory();
			XaTransactionFactory tf = new BerkeleyDbTransactionFactory();

			List<Pair<TransactionInterceptorProvider, Object>> providers
			= new ArrayList<Pair<TransactionInterceptorProvider, Object>>( 2 );

			//XXX: replace null by providers
			xaContainer = XaContainer.create( this, baseStorePath + "/logical.log", cf, tf, null, params );
			try {
				xaContainer.openLogicalLog();
			} catch ( IOException e ) {
				throw new RuntimeException( "Unable to open bekeleydb log in " + baseStorePath, e );
			}

			Object keep = params.get( "keep_logical_logs" );
			xaContainer.getLogicalLog().setKeepLogs( shouldKeepLog( keep!=null?(String)keep:"false", DEFAULT_NAME ) );
			setLogicalLogAtCreationTime( xaContainer.getLogicalLog() );
		} else {
			xaContainer = null;
		}

	}


	static Pair<String, Boolean> getStoreDir( String dbStoreDir ) {
		File dir = new File( dbStoreDir );
		boolean created = false;
		if ( !dir.exists() ) {
			if ( !dir.mkdirs() ) {
				throw new RuntimeException( "Unable to create directory path[" + dir.getAbsolutePath() + "] for Neo4j store." );
			}
			created = true;
		}
		if ( dir.list().length == 0 ) {
			created = true;
		}
		return Pair.of( dir.getAbsolutePath(), created );
	}


	static IndexProviderStore newIndexStore( String dbStoreDir ) {
		// FIXME: is this really correct? doesn't seem safe...
		//return new IndexProviderStore( new File( dbStoreDir, "store.db" ), CommonFactories.defaultFileSystemAbstraction() );
		return new IndexProviderStore( new File( dbStoreDir, "store.db" ), CommonFactories.defaultFileSystemAbstraction(), 1, true );
	}


	@Override
	public void close() {
		//System.err.println( "close of " + this.getClass() );
		if ( closed ) {
			return;
		}
		// TODO
		if ( null != xaContainer ) {
			xaContainer.close();
		}
		store.close();
		try {
			// berkeleyDb.close();
			for ( Map<String, Database> dbs : databases.values() ) {
				for ( Database db : dbs.values() ) {
					if ( db.getEnvironment().isValid() ) {
						//System.err.println( "bdb environ closing:" + db.getEnvironment().getHome() );
						db.close();
						db.getEnvironment().close();
					}
				}
			}
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		closed = true;
	}


	@Override
	public XaConnection getXaConnection() {
		return new BerkeleyDbXaConnection( baseStorePath, xaContainer.getResourceManager(), getBranchId() );
	}

	private class BerkeleyDbCommandFactory extends XaCommandFactory {

		BerkeleyDbCommandFactory() {
			super();
		}


		@Override
		public XaCommand readCommand( ReadableByteChannel channel, ByteBuffer buffer ) throws IOException {
			return BerkeleyDbCommand.readCommand( channel, buffer, BerkeleyDbDataSource.this );
		}
	}

	private class BerkeleyDbTransactionFactory extends XaTransactionFactory {

		@Override
		public XaTransaction create( int identifier ) {
			return createTransaction( identifier, this.getLogicalLog() );
		}


		@Override
		public void flushAll() {
			// Not much we can do...
		}


		@Override
		public long getCurrentVersion() {
			return store.getVersion();
		}


		@Override
		public long getAndSetNewVersion() {
			return store.incrementVersion();
		}


		@Override
		public long getLastCommittedTx() {
			return store.getLastCommittedTx();
		}
	}


	void getReadLock() {
		lock.readLock().lock();
	}


	void releaseReadLock() {
		lock.readLock().unlock();
	}


	void getWriteLock() {
		lock.writeLock().lock();
	}


	void releaseWriteLock() {
		lock.writeLock().unlock();
	}


	XaTransaction createTransaction( int identifier, XaLogicalLog logicalLog ) {
		return new BerkeleydbTransaction( identifier, logicalLog, this );
	}


	@Override
	public long getCreationTime() {
		return store.getCreationTime();
	}


	@Override
	public long getRandomIdentifier() {
		return store.getRandomNumber();
	}


	@Override
	public long getCurrentLogVersion() {
		return store.getVersion();
	}


	public static byte[] indexKey( String key, Object value ) {
		if (value instanceof byte[]) {
			return (byte[]) value;
		}
		if (value instanceof String) {
			return ((String) value).getBytes();
		}
		return String.valueOf( "" + value ).getBytes();
	}

	//get key-value database
	public Database getDatabase( IndexIdentifier identifier, Object key ) {
		Map<String, Database> db = databases.get( identifier );
		if ( null == db ) {
			db = new HashMap<String, Database>();
			databases.put( identifier, db );
		}
		Database result = db.get( key.toString() );
		if ( null == result ) {
			result = createDB( identifier, key );
			db.put( key.toString(), result );
		}

		return result;
	}

	//get entity store
	public EntityStore getEntityStore( IndexIdentifier identifier, Object key ) {
		Map<String, EntityStore> stores = entityStores.get( identifier );
		if ( null == stores ) {
			stores = new HashMap<String, EntityStore>();
			entityStores.put( identifier, stores );
		}
		EntityStore result = stores.get( key.toString() );
		if ( null == result ) {
			result = createEntityStore( identifier, key );
			stores.put( key.toString(), result );
		}

		return result;
	}

	public void addEntry( Database db, IndexIdentifier identifier, long[] entityIds, String key, Object value ) {
		byte[] indexKey = indexKey( key, value );
		long[] existingIds = getExistingIds( db, indexKey );
		long[] ids = ArrayUtil.include( existingIds, entityIds );
		try {
			db.put( null, new DatabaseEntry( indexKey ), new DatabaseEntry( ArrayUtil.toBytes( ids ) ) );
		} catch ( DatabaseException e ) {
			e.printStackTrace();
		}
	}


	private long[] getExistingIds( Database db, byte[] key ) {
		try {
			DatabaseEntry value = new DatabaseEntry();
			db.get( null, new DatabaseEntry( key ), value, LockMode.READ_UNCOMMITTED );

			return value.getData() != null ? ArrayUtil.toLongArray( value.getData() ) : new long[0];
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}


	public void removeEntry( Database db, IndexIdentifier identifier, long[] entityIds, String key, Object value ) {
		byte[] indexKey = indexKey( key, value );
		long[] existingIds = getExistingIds( db, indexKey );
		long[] ids = ArrayUtil.exclude( existingIds, entityIds );
		if ( ids.length == 0 ) {
			db.removeSequence( null, new DatabaseEntry( BerkeleyDbDataSource.indexKey( key, value ) ) );
		} else {
			db.put( null, new DatabaseEntry( BerkeleyDbDataSource.indexKey( key, value ) ),
					new DatabaseEntry( ArrayUtil.toBytes( ids ) ) );
		}
	}


	public void commit( Database db ) {
		db.sync();
	}


	@Override
	public long getLastCommittedTxId() {
		return store.getLastCommittedTx();
	}


	private Database createDB( IndexIdentifier identifier, Object key ) {
		try {
			EnvironmentConfig environmentConfig = new EnvironmentConfig();
			environmentConfig.setAllowCreate( true );
			// environmentConfig.setConfigParam( "java.util.logging.level",
			// "INFO" );
			// perform other environment configurations
			String dir =
					BerkeleyDbDataSource.getStoreDir(
							baseStorePath + "/index/bdb/" + identifier.itemClass.getSimpleName() + "/" + identifier.indexName + "/"
									+ key ).first();
			//System.err.println( "bdb environ opening:" + dir );
			Environment environment = new Environment( new File( dir ), environmentConfig );
			environmentConfig.setTransactional( false );
			DatabaseConfig databaseConfig = new DatabaseConfig();
			databaseConfig.setAllowCreate( true );
			// perform other database configurations
			Database db = environment.openDatabase( null, key.toString(), databaseConfig );
			return db;
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	private EntityStore createEntityStore( IndexIdentifier identifier, Object key ) {
		try {
			String dir =
					BerkeleyDbDataSource.getStoreDir(
							baseStorePath + "/index/bdb/" + identifier.itemClass.getSimpleName() + "/" + identifier.indexName + "/"
									+ key ).first();

			File envHome = new File(dir);

			EnvironmentConfig myEnvConfig = new EnvironmentConfig();
			StoreConfig storeConfig = new StoreConfig();

			myEnvConfig.setAllowCreate(!isReadOnly);
			storeConfig.setAllowCreate(!isReadOnly);

			Environment myEnv = new Environment(envHome, myEnvConfig);
			return new EntityStore(myEnv, "RelationshipStore", storeConfig);
		} catch ( Exception e ) {
			throw new RuntimeException( e );
		}
	}

	public Map<IndexIdentifier, Map<String, Database>> getDatabases() {
		return databases;
	}

	public Map<IndexIdentifier, Map<String, EntityStore>> getEntityStores() {
		return entityStores;
	}
}