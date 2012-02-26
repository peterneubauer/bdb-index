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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.index.IndexHits;

/**
 * A simple implementation of an {@link IndexHits} where the size is known at
 * construction time.
 *
 * @param <T> the type of items.
 */
public class IndexHitsImpl<SpatialDatabaseRecord> implements IndexHits<SpatialDatabaseRecord> {
	private final Iterator<SpatialDatabaseRecord> _hits;
	private final int _size;

	/**
	 * Wraps an Iterable<T> with a known size.
	 * 
	 * @param hits the hits to iterate through.
	 * @param size the size of the iteration.
	 */
	public IndexHitsImpl( Iterable<SpatialDatabaseRecord> hits, int size ) {
		this( hits.iterator(), size );
	}

	/**
	 * Wraps an Iterator<T> with a known size.
	 * 
	 * @param hits the hits to iterate through.
	 * @param size the size of the iteration.
	 */
	public IndexHitsImpl( Iterator<SpatialDatabaseRecord> hits, int size ) {
		_hits = hits;
		_size = size;
	}

	@Override
	public Iterator<SpatialDatabaseRecord> iterator() {
		return _hits;
	}

	@Override
	public int size() {
		return _size;
	}

	@Override
	public void close() {
		// Do nothing
	}

	@Override
	public boolean hasNext() {
		return _hits.hasNext();
	}

	@Override
	public SpatialDatabaseRecord next() {
		return _hits.next();
	}

	@Override
	public void remove() {
		_hits.remove();
	}

	@Override
	public SpatialDatabaseRecord getSingle() {
		SpatialDatabaseRecord result = _hits.hasNext() ? _hits.next() : null;
		if ( _hits.hasNext() ) {
			throw new NoSuchElementException();
		}
		return result;
	}

	@Override
	public float currentScore() {
		return 0;
	}
}
