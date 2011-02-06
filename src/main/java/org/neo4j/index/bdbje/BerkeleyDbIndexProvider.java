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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.kernel.impl.index.IndexConnectionBroker;
import org.neo4j.kernel.impl.index.ReadOnlyIndexConnectionBroker;

@Service.Implementation( KernelExtension.class )
public class BerkeleyDbIndexProvider extends IndexProvider
{
    static final String KEY_PROVIDER = "provider";

    public static final String SERVICE_NAME = "berkeleydb-je";
    public static final Map<String, String> DEFAULT_CONFIG = Collections.unmodifiableMap( MapUtil.stringMap(
                    KEY_PROVIDER, SERVICE_NAME ) );
    private GraphDatabaseService graphDb;
    private IndexConnectionBroker<BerkeleyDbXaConnection> broker;
    private BerkeleyDbDataSource dataSource;
    
    public BerkeleyDbIndexProvider()
    {
        super( "berkeleydb-je" );
    }
    
    public BerkeleyDbIndexProvider( GraphDatabaseService db )
    {
        this();
        load( db, ((AbstractGraphDatabase) db).getConfig() );
    }
    
    IndexConnectionBroker<BerkeleyDbXaConnection> broker()
    {
        return this.broker;
    }
    
    GraphDatabaseService graphDb()
    {
        return this.graphDb;
    }
    
    BerkeleyDbDataSource dataSource()
    {
        return this.dataSource;
    }
    
    @Override
    protected void load( KernelData kernel )
    {
        try
        {
            load( kernel.graphDatabase(), kernel.getConfig() );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            throw e;
        }
    }
    
    private void load( GraphDatabaseService db, Config config )
    {
        this.graphDb = db;
        boolean isReadOnly = ((AbstractGraphDatabase) graphDb).isReadOnly();
        Map<Object, Object> params = new HashMap<Object, Object>( config.getParams() );
        params.put( "read_only", isReadOnly );
        this.dataSource = (BerkeleyDbDataSource) config.getTxModule().registerDataSource(
                BerkeleyDbDataSource.DEFAULT_NAME, BerkeleyDbDataSource.class.getName(),
                BerkeleyDbDataSource.DEFAULT_BRANCH_ID, params, true );
        this.broker = isReadOnly ?
                new ReadOnlyIndexConnectionBroker<BerkeleyDbXaConnection>( config.getTxModule().getTxManager() ) :
                new ConnectionBroker( config.getTxModule().getTxManager(), dataSource );
    }

    @Override
    public Index<Node> nodeIndex( String indexName, Map<String, String> config )
    {
        return new BerkeleyDbIndex.NodeIndex( this, new IndexIdentifier( Node.class, indexName ) );
    }

    @Override
    public RelationshipIndex relationshipIndex( String indexName,
            Map<String, String> config )
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        return config;
    }

    @Override
    public String getDataSourceName()
    {
        return BerkeleyDbDataSource.DEFAULT_NAME;
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig,
            Map<String, String> config )
    {
        
        String provider = "provider";
        return storedConfig.get( provider ).equals( config.get(provider));
    }
}
