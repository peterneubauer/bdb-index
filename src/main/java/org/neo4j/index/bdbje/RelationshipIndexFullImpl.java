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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexHits;

import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;

public class RelationshipIndexFullImpl implements org.neo4j.graphdb.index.RelationshipIndex {

	final BerkeleyDbIndexImplementation	service;
	final IndexIdentifier identifier;

	RelationshipIndexFullImpl(BerkeleyDbIndexImplementation implementation, String name) {

		service = implementation;

		identifier = new IndexIdentifier(Relationship.class, name);
	}

	@Override
	public IndexHits<Relationship> get(String key, Object valueOrNull,
			Node startNodeOrNull, Node endNodeOrNull) {

		EntityStore store = service.dataSource().getEntityStore(identifier, key);

		PrimaryIndex<Long,RelationshipEntity> rId =
				store.getPrimaryIndex(Long.class, RelationshipEntity.class);

		SecondaryIndex<String,Long,RelationshipEntity> sNid =
				store.getSecondaryIndex(rId, String.class, "sNodeId");

		return null;
	}

	@Override
	public IndexHits<Relationship> query(String key,
			Object queryOrQueryObjectOrNull, Node startNodeOrNull,
			Node endNodeOrNull) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexHits<Relationship> query(Object queryOrQueryObjectOrNull,
			Node startNodeOrNull, Node endNodeOrNull) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getName() {
		return "bdb-relationships-full";
	}

	@Override
	public Class<Relationship> getEntityType() {
		return Relationship.class;
	}

	@Override
	public IndexHits<Relationship> get(String key, Object value) {

		EntityStore store = service.dataSource().getEntityStore(identifier, key);

		PrimaryIndex<Long,RelationshipEntity> index =
				store.getPrimaryIndex(Long.class, RelationshipEntity.class);

		SecondaryIndex<String,Long,RelationshipEntity> sIndex =
				store.getSecondaryIndex(index, String.class, "value");

		return new EntityIndexHits( sIndex.subIndex(value.toString()).entities() );
	}

	@Override
	public IndexHits<Relationship> query(String key, Object queryOrQueryObject) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IndexHits<Relationship> query(Object queryOrQueryObject) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isWriteable() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void add(Relationship entity, String key, Object value) {
		EntityStore store = service.dataSource().getEntityStore(identifier, key);

		PrimaryIndex<Long,RelationshipEntity> index =
				store.getPrimaryIndex(Long.class, RelationshipEntity.class);

		index.put(RelationshipEntity.of(entity, key, value.toString()));
	}

	@Override
	public void remove(Relationship entity, String key, Object value) {
		EntityStore store = service.dataSource().getEntityStore(identifier, key);

		PrimaryIndex<Long,RelationshipEntity> index =
				store.getPrimaryIndex(Long.class, RelationshipEntity.class);

		SecondaryIndex<String,Long,RelationshipEntity> sIndex =
				store.getSecondaryIndex(index, String.class, "value");

		EntityCursor<RelationshipEntity> entities = sIndex.subIndex(value.toString()).entities();

		sIndex.subIndex(value.toString()).delete(entity.getId());
	}

	@Override
	public void remove(Relationship entity, String key) {
		// TODO Auto-generated method stub

	}

	@Override
	public void remove(Relationship entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete() {
		// TODO Auto-generated method stub

	}

	private byte[] getBytes(Object obj) {
		if (obj instanceof String) {
			return ((String) obj).getBytes();
		}
		return (byte[])obj;

	}

	private String getString(Object obj) {
		if (obj instanceof String) {
			return (String) obj;

		} else if (obj instanceof Number) {
			return ((Number) obj).toString();

		}
		throw new IllegalArgumentException();

	}

	Relationship getRelationship(RelationshipEntity entity) {
		return service.graphDb().getRelationshipById(entity.id);
	}

	class EntityIndexHits implements IndexHits<Relationship> {

		EntityCursor<RelationshipEntity> cursor;

		Iterator<RelationshipEntity> it;

		EntityIndexHits(EntityCursor<RelationshipEntity> c) {
			cursor = c;

			it = cursor.iterator();
		}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public Relationship next() {
			RelationshipEntity entity = it.next();
			return getRelationship(entity);
		}

		@Override
		public void remove() {
			// Auto-generated method stub
		}

		@Override
		public Iterator<Relationship> iterator() {
			return this;
		}

		@Override
		public int size() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void close() {
			cursor.close();
		}

		@Override
		public Relationship getSingle() {
			//XXX: finish

			if (!hasNext()) {
				return null;
			}

			Relationship r = next();
			if (hasNext()) {
				throw new NoSuchElementException();
			}
			return r;
		}

		@Override
		public float currentScore() {
			return 1;
		}
	}

	class SingleHit implements IndexHits<Relationship> {

		final long id;
		boolean hasNext = true;

		SingleHit(RelationshipEntity entity) {
			id = entity.id;
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public Relationship next() {
			if (hasNext) {
				hasNext = false;
				return service.graphDb().getRelationshipById(id);
			}
			throw new NoSuchElementException();
		}

		@Override
		public void remove() {
			// Auto-generated method stub
		}

		@Override
		public Iterator<Relationship> iterator() {
			return this;
		}

		@Override
		public int size() {
			return 1;
		}

		@Override
		public void close() {
			// Auto-generated method stub
		}

		@Override
		public Relationship getSingle() {
			hasNext = false;
			return service.graphDb().getRelationshipById(id);
		}

		@Override
		public float currentScore() {
			return 1;
		}

	}
}
