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

import org.neo4j.graphdb.PropertyContainer;

class IndexIdentifier
{
    final Class<? extends PropertyContainer> itemClass;
    final String indexName;
    
    IndexIdentifier( Class<? extends PropertyContainer> cls, String name )
    {
        this.itemClass = cls;
        this.indexName = name;
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( o == null || !getClass().equals( o.getClass() ) )
        {
            return false;
        }
        IndexIdentifier i = (IndexIdentifier) o;
        return itemClass.equals( i.itemClass ) &&
                indexName.equals( i.indexName );
    }
    
    @Override
    public int hashCode()
    {
        int code = 17;
        code += 7*itemClass.hashCode();
        code += 7*indexName.hashCode();
        return code;
    }
}
