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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TxData
{
    private Map<String, Map<Object, Set<Long>>> data;
    
    void add( Long entityId, String key, Object value )
    {
        idCollection( key, value, true ).add( entityId );
    }
    
    private Set<Long> idCollection( String key, Object value, boolean create )
    {
        Map<Object, Set<Long>> keyMap = keyMap( key, create );
        if ( keyMap == null )
        {
            return null;
        }
        
        Set<Long> ids = keyMap.get( value );
        if ( ids == null && create )
        {
            ids = new HashSet<Long>();
            keyMap.put( value, ids );
        }
        return ids;
    }

    private Map<Object, Set<Long>> keyMap( String key, boolean create )
    {
        if ( data == null )
        {
            if ( create )
            {
                data = new HashMap<String, Map<Object,Set<Long>>>();
            }
            else
            {
                return null;
            }
        }
        
        Map<Object, Set<Long>> inner = data.get( key );
        if ( inner == null && create )
        {
            inner = new HashMap<Object, Set<Long>>();
            data.put( key, inner );
        }
        return inner;
    }

    void close()
    {
    }

    void remove( Long entityId, String key, Object value )
    {
        if ( data == null )
        {
            return;
        }
        Collection<Long> ids = idCollection( key, value, false );
        if ( ids != null )
        {
            ids.remove( entityId );
        }
    }

    Set<Long> getEntityIds( String key, Object value )
    {
        Set<Long> ids = idCollection( key, value, false );
        if ( ids == null )
        {
            return Collections.emptySet();
        }
        return ids;
    }
    
    Map<String, Map<Object, Set<Long>>> rawMap()
    {
        return this.data;
    }
}
