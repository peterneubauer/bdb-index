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

import java.util.Map;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.impl.index.IndexXaConnection;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceHelpImpl;
import org.neo4j.kernel.impl.transaction.xaframework.XaResourceManager;

/**
 * An XA connection used with {@link BerkeleyDbDataSource}.
 * This class is public because the XA framework requires it.
 */
class BerkeleyDbXaConnection extends IndexXaConnection
{
    private final BerkeleyDbXaResource xaResource;

    BerkeleyDbXaConnection( Object identifier, XaResourceManager xaRm,
        byte[] branchId )
    {
        super( xaRm );
        xaResource = new BerkeleyDbXaResource( identifier, xaRm, branchId );
    }

    @Override
    public XAResource getXaResource()
    {
        return xaResource;
    }

    private static class BerkeleyDbXaResource extends XaResourceHelpImpl
    {
        private final Object identifier;

        BerkeleyDbXaResource( Object identifier, XaResourceManager xaRm,
            byte[] branchId )
        {
            super( xaRm, branchId );
            this.identifier = identifier;
        }

        @Override
        public boolean isSameRM( XAResource xares )
        {
            if ( xares instanceof BerkeleyDbXaResource )
            {
                return identifier.equals(
                    ((BerkeleyDbXaResource) xares).identifier );
            }
            return false;
        }
    }

    private BerkeleydbTransaction tx;

    BerkeleydbTransaction getTx()
    {
        if ( tx == null )
        {
            try
            {
                tx = ( BerkeleydbTransaction ) getTransaction();
            }
            catch ( XAException e )
            {
                throw new RuntimeException( "Unable to get lucene tx", e );
            }
        }
        return tx;
    }

    <T extends PropertyContainer> void add( BerkeleyDbIndex<T> index,
            T entity, String key, Object value )
    {
        getTx().add( index, entity, key, value );
    }

    <T extends PropertyContainer> void remove( BerkeleyDbIndex<T> index,
            T entity, String key, Object value )
    {
        getTx().remove( index, entity, key, value );
    }

    @Override
    public void createIndex( Class<? extends PropertyContainer> entityType, String indexName,
            Map<String, String> config )
    {
        getTx().create( entityType, indexName, config );
    }
}
