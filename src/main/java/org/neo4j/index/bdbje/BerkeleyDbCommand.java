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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.util.IoPrimitiveUtils;

abstract class BerkeleyDbCommand extends XaCommand
{
    private static final byte ADD_COMMAND = (byte) 1;
    private static final byte REMOVE_COMMAND = (byte) 2;
    private static final byte CREATE_COMMAND = (byte) 3;

    static final byte NODE = (byte) 1;
    static final byte RELATIONSHIP = (byte) 2;

    final IndexIdentifier indexId;
    final long[] entityIds;
    final String key;
    final String value;
    final byte commandValue;

    BerkeleyDbCommand( byte commandValue, IndexIdentifier indexId, long[] entityIds, String key,
            String value )
    {
        this.commandValue = commandValue;
        this.indexId = indexId;
        this.entityIds = entityIds;
        this.key = key;
        this.value = value;
    }

    public byte getEntityType()
    {
        if ( indexId.itemClass == Node.class )
        {
            return NODE;
        }
        else if ( indexId.itemClass == Relationship.class )
        {
            return RELATIONSHIP;
        }
        throw new IllegalArgumentException( indexId.itemClass.toString() );
    }

    @Override
    public void execute()
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void writeToFile( LogBuffer buffer ) throws IOException
    {
        buffer.put( commandValue );
        buffer.put( getEntityType() );
        char[] indexName = indexId.indexName.toCharArray();
        buffer.putInt( indexName.length );
        buffer.putInt( entityIds.length );
        char[] key = this.key.toCharArray();
        buffer.putInt( key.length );
        char[] value = this.value.toCharArray();
        buffer.putInt( value.length );
        buffer.put( indexName );
        for ( long id : entityIds )
        {
            buffer.putLong( id );
        }
        buffer.put( key );
        buffer.put( value );
    }

    static class AddCommand extends BerkeleyDbCommand
    {
        AddCommand( IndexIdentifier indexId, long[] entityIds, String key, String value )
        {
            super( ADD_COMMAND, indexId, entityIds, key, value );
        }
    }

    static class RemoveCommand extends BerkeleyDbCommand
    {
        RemoveCommand( IndexIdentifier indexId, long[] entityIds, String key, String value )
        {
            super( REMOVE_COMMAND, indexId, entityIds, key, value );
        }
    }

    static class CreateCommand extends BerkeleyDbCommand
    {
        // static final IndexIdentifier FAKE_IDENTIFIER = new IndexIdentifier(
        // null, null );
        static final long[] EMPTY_IDS = new long[0];
        final Map<String, String> config;

        CreateCommand( IndexIdentifier identifier, Map<String, String> config )
        {
            super( CREATE_COMMAND, identifier, EMPTY_IDS, "", "" );
            this.config = config;
        }

        @Override
        public void writeToFile( LogBuffer buffer ) throws IOException
        {
            super.writeToFile( buffer );
            buffer.putInt( config.size() );
            for ( Map.Entry<String, String> entry : config.entrySet() )
            {
                writeLengthAndString( buffer, entry.getKey() );
                writeLengthAndString( buffer, entry.getValue() );
            }
        }
    }

    private static void writeLengthAndString( LogBuffer buffer, String string ) throws IOException
    {
        char[] chars = string.toCharArray();
        buffer.putInt( chars.length );
        buffer.put( chars );
    }

    static XaCommand readCommand( ReadableByteChannel channel, ByteBuffer buffer,
            BerkeleyDbDataSource dataSource ) throws IOException
    {
        buffer.clear();
        buffer.limit( 18 );
        if ( channel.read( buffer ) != buffer.limit() )
        {
            return null;
        }
        buffer.flip();
        byte commandType = buffer.get();
        byte cls = buffer.get();
        Class<? extends PropertyContainer> itemsClass = null;
        switch ( cls )
        {
        case NODE:
            itemsClass = Node.class;
            break;
        case RELATIONSHIP:
            itemsClass = Relationship.class;
            break;
        default:
            return null;
        }

        int indexNameLength = buffer.getInt();
        int numEntities = buffer.getInt();
        int keyCharLength = buffer.getInt();
        int valueCharLength = buffer.getInt();

        long[] entityIds = new long[numEntities];
        for ( int i = 0; i < numEntities; i++ )
        {
            entityIds[i] = IoPrimitiveUtils.readLong( channel, buffer );
        }

        String indexName = IoPrimitiveUtils.readString( channel, buffer, indexNameLength );
        if ( indexName == null )
        {
            return null;
        }

        String key = IoPrimitiveUtils.readString( channel, buffer, keyCharLength );
        if ( key == null )
        {
            return null;
        }

        String value = IoPrimitiveUtils.readString( channel, buffer, valueCharLength );
        if ( value == null )
        {
            return null;
        }

        Map<String, String> creationConfig = null;
        if ( commandType == CREATE_COMMAND )
        {
            creationConfig = IoPrimitiveUtils.readMap( channel, buffer );
        }

        IndexIdentifier identifier = new IndexIdentifier( itemsClass, indexName );
        switch ( commandType )
        {
        case ADD_COMMAND:
            return new AddCommand( identifier, entityIds, key, value );
        case REMOVE_COMMAND:
            return new RemoveCommand( identifier, entityIds, key, value );
        case CREATE_COMMAND:
            return new CreateCommand( identifier, creationConfig );
        default:
            return null;
        }
    }
}
