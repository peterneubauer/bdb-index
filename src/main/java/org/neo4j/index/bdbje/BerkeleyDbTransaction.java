/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.index.bdbje.BerkeleyDbCommand.AddCommand;
import org.neo4j.index.bdbje.BerkeleyDbCommand.CreateCommand;
import org.neo4j.index.bdbje.BerkeleyDbCommand.RemoveCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.impl.transaction.xaframework.XaTransaction;

import com.sleepycat.je.Database;

class BerkeleydbTransaction extends XaTransaction
{
	private final Map<IndexIdentifier, TxDataBoth> txData =
			new HashMap<IndexIdentifier, TxDataBoth>();
	private final BerkeleyDbDataSource dataSource;

	private final Map<IndexIdentifier,Collection<BerkeleyDbCommand>> commandMap =
			new HashMap<IndexIdentifier,Collection<BerkeleyDbCommand>>();

	BerkeleydbTransaction( int identifier, XaLogicalLog xaLog,
			BerkeleyDbDataSource luceneDs )
			{
		super( identifier, xaLog );
		dataSource = luceneDs;
			}

	<T extends PropertyContainer> void add( BerkeleyDbIndex<T> index, T entity,
			String key, Object value )
	{
		TxDataBoth data = getTxData( index, true );
		insert( index, entity, key, value, data.added( true ), data.removed( false ) );
	}

	private long getEntityId( PropertyContainer entity )
	{
		return entity instanceof Node ? ((Node) entity).getId() :
			((Relationship) entity).getId();
	}

	<T extends PropertyContainer> TxDataBoth getTxData( BerkeleyDbIndex<T> index, boolean createIfNotExists ) {
		IndexIdentifier identifier = index._identifier;
		TxDataBoth data = txData.get( identifier );
		if ( data == null && createIfNotExists )
		{
			data = new TxDataBoth( index );
			txData.put( identifier, data );
		}
		return data;
	}

	<T extends PropertyContainer> void remove( BerkeleyDbIndex<T> index, T entity, String key, Object value ) {
		TxDataBoth data = getTxData( index, true );
		insert( index, entity, key, value, data.removed( true ), data.added( false ) );
	}

	private void queueCommand( BerkeleyDbCommand command )
	{
		IndexIdentifier indexId = command._indexId;
		Collection<BerkeleyDbCommand> commands = commandMap.get( indexId );
		if ( commands == null )
		{
			commands = new ArrayList<BerkeleyDbCommand>();
			commandMap.put( indexId, commands );
		}
		commands.add( command );
	}

	private <T extends PropertyContainer> void insert( BerkeleyDbIndex<T> index,
			T entity, String key, Object value, TxData insertInto, TxData removeFrom )
	{
		long id = getEntityId( entity );
		if ( removeFrom != null )
		{
			removeFrom.remove( id, key, value );
		}
		insertInto.add( id, key, value );
	}

	<T extends PropertyContainer> Set<Long> getRemovedIds( BerkeleyDbIndex<T> index,
			String key, Object value )
			{
		TxData removed = removedTxDataOrNull( index );
		if ( removed == null )
		{
			return Collections.emptySet();
		}
		Set<Long> ids = removed.getEntityIds( key, value );
		return ids != null ? ids : Collections.<Long>emptySet();
			}

	<T extends PropertyContainer> Set<Long> getAddedIds( BerkeleyDbIndex<T> index,
			String key, Object value )
			{
		TxData added = addedTxDataOrNull( index );
		if ( added == null )
		{
			return Collections.emptySet();
		}
		Set<Long> ids = added.getEntityIds( key, value );
		return ids != null ? ids : Collections.<Long>emptySet();
			}

	private <T extends PropertyContainer> TxData addedTxDataOrNull( BerkeleyDbIndex<T> index )
	{
		TxDataBoth data = getTxData( index, false );
		if ( data == null )
		{
			return null;
		}
		return data.added( false );
	}

	private <T extends PropertyContainer> TxData removedTxDataOrNull( BerkeleyDbIndex<T> index )
	{
		TxDataBoth data = getTxData( index, false );
		if ( data == null )
		{
			return null;
		}
		return data.removed( false );
	}

	@Override
	protected void doAddCommand( XaCommand command )
	{ // we override inject command and manage our own in memory command list
	}

	@Override
	protected void injectCommand( XaCommand command )
	{
		queueCommand( ( BerkeleyDbCommand ) command );
	}

	@Override
	protected void doCommit()
	{
		dataSource.getWriteLock();
		try
		{
			for ( Map.Entry<IndexIdentifier, Collection<BerkeleyDbCommand>> entry :
				commandMap.entrySet() )
			{
				IndexIdentifier identifier = entry.getKey();
				Collection<BerkeleyDbCommand> commandList = entry.getValue();
				for ( BerkeleyDbCommand command : commandList )
				{
					if ( command instanceof CreateCommand )
					{
						dataSource.indexStore.setIfNecessary(
								command._indexId.itemClass,
								command._indexId.indexName,
								((CreateCommand) command)._config );
						continue;
					}

					long[] entityIds = command._entityIds;
					String key = command._key;
					String value = command._value;
					Database db = dataSource.getDatabase( identifier, key );

					if ( command instanceof AddCommand ) {
						dataSource.addEntry( db, identifier, entityIds, key, value );

					} else if ( command instanceof RemoveCommand ) {
						dataSource.removeEntry( db, identifier, entityIds, key, value );

					} else {
						throw new RuntimeException( "Unknown command type " + command + ", " + command.getClass() );
					}
				}
				//                dataSource.commit( db );
			}
			closeTxData();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			throw new RuntimeException( e );
		}
		catch ( Throwable e )
		{
			e.printStackTrace();
		}
		finally
		{
			dataSource.releaseWriteLock();
		}
	}

	private void closeTxData()
	{
		for ( TxDataBoth data : txData.values() )
		{
			data.close();
		}
		txData.clear();
	}

	@Override
	protected void doPrepare()
	{
		for ( TxDataBoth _txData : txData.values() )
		{
			if ( _txData.add != null )
			{
				for ( Map.Entry<String, Map<Object, Set<Long>>> keyMap :
					_txData.add.rawMap().entrySet() )
				{
					String key = keyMap.getKey();
					for ( Map.Entry<Object, Set<Long>> valueMap : keyMap.getValue().entrySet() )
					{
						AddCommand command = new AddCommand( _txData.index._identifier,
								ArrayUtil.toPrimitiveLongArray( valueMap.getValue() ), key,
								valueMap.getKey().toString() );
						addCommand( command );
						queueCommand( command );
					}
				}
			}
		}

		// TODO Fix duplicate code for-loop
		for ( TxDataBoth txData : this.txData.values() )
		{
			if ( txData.remove != null )
			{
				for ( Map.Entry<String, Map<Object, Set<Long>>> keyMap :
					txData.remove.rawMap().entrySet() )
				{
					String key = keyMap.getKey();
					for ( Map.Entry<Object, Set<Long>> valueMap : keyMap.getValue().entrySet() )
					{
						RemoveCommand command = new RemoveCommand( txData.index._identifier,
								ArrayUtil.toPrimitiveLongArray( valueMap.getValue() ), key,
								valueMap.getKey().toString() );
						addCommand( command );
						queueCommand( command );
					}
				}
			}
		}
	}

	@Override
	protected void doRollback()
	{
		// TODO Auto-generated method stub
		commandMap.clear();
		closeTxData();
	}

	@Override
	public boolean isReadOnly()
	{
		for ( TxDataBoth data : txData.values() )
		{
			if ( data.add != null || data.remove != null )
			{
				return false;
			}
		}
		return true;
	}

	// Bad name
	private class TxDataBoth
	{
		private TxData add;
		private TxData remove;
		@SuppressWarnings("unchecked")
		private final BerkeleyDbIndex index;

		@SuppressWarnings("unchecked")
		public TxDataBoth( BerkeleyDbIndex index )
		{
			this.index = index;
		}

		TxData added( boolean createIfNotExists )
		{
			if ( add == null && createIfNotExists )
			{
				add = new TxData();
			}
			return add;
		}

		TxData removed( boolean createIfNotExists )
		{
			if ( remove == null && createIfNotExists )
			{
				remove = new TxData();
			}
			return remove;
		}

		void close()
		{
			safeClose( add );
			safeClose( remove );
		}

		private void safeClose( TxData data )
		{
			if ( data != null )
			{
				data.close();
			}
		}
	}

	void create( Class<? extends PropertyContainer> entityType, String indexName,
			Map<String, String> config )
	{
		queueCommand( new CreateCommand( new IndexIdentifier( entityType, indexName ), config ) );
	}
}
