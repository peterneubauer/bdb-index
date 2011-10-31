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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

abstract class ArrayUtil
{
	public static byte[] toBytes( long... values )
	{
		byte[] array = new byte[8*values.length];
		for ( int i = 0; i < values.length; i++ )
		{
			toBytes( values[i], array, 8*i );
		}
		return array;
	}

	public static byte[] toBytes( long value )
	{
		return toBytes( value, new byte[8], 0 );
	}

	public static byte[] toBytes( long value, byte[] target, int offset )
	{
		for ( int i = 0; i < 8; i++ )
		{
			target[7-i + offset] = (byte) (value >>> (i * 8));
		}
		return target;
	}

	public static long toLong( byte[] array, int offset )
	{
		long value = 0;
		for ( int i = offset; i < offset+8; i++ )
		{
			value <<= 8;
			value ^= (long) array[i] & 0xFF;
		}
		return value;
	}

	public static long[] toLongArray( byte[] array )
	{
		long[] result = new long[array.length/8];
		for ( int i = 0; i < result.length; i++ )
		{
			result[i] = toLong( array, i*8 );
		}
		return result;
	}

	public static long[] include( long[] existingIds, long[] entityIds )
	{
		Set<Long> added = new HashSet<Long>();
		Collection<Long> ids = new ArrayList<Long>();
		for ( Long id : existingIds )
		{
			if ( added.add( id ) )
			{
				ids.add( id );
			}
		}
		for ( Long id : entityIds )
		{
			if ( added.add( id ) )
			{
				ids.add( id );
			}
		}
		return ArrayUtil.toPrimitiveLongArray( ids );
	}

	public static long[] exclude( long[] existingIds, long[] entityIds )
	{
		Set<Long> entityIdsSet = new HashSet<Long>();
		for ( Long id : entityIds )
		{
			entityIdsSet.add( id );
		}
		Collection<Long> ids = new ArrayList<Long>();
		for ( long id : existingIds )
		{
			if ( !entityIdsSet.contains( id ) )
			{
				ids.add( id );
			}
		}
		return toPrimitiveLongArray( ids );
	}

	public static long[] toPrimitiveLongArray( Collection<Long> ids )
	{
		long[] result = new long[ids.size()];
		int i = 0;
		for ( Long id : ids )
		{
			result[i++] = id;
		}
		return result;
	}
}
