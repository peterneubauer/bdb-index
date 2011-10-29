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

import org.neo4j.graphdb.Relationship;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

import static com.sleepycat.persist.model.Relationship.MANY_TO_MANY;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
@Entity
class RelationshipEntity {

	@PrimaryKey
	final long id;

	@SecondaryKey(relate=MANY_TO_MANY)//,relatedEntity=Node.class
	final long sNodeId;

	@SecondaryKey(relate=MANY_TO_MANY)//,relatedEntity=Node.class
	final long eNodeId;

	RelationshipEntity(Relationship r) {
		id = r.getId();
		sNodeId = r.getStartNode().getId();
		eNodeId = r.getEndNode().getId();
	}

	public static RelationshipEntity of(Relationship r) {
		return new RelationshipEntity(r);
	}

	@Override
	public boolean equals( Object obj ) {
		return ((RelationshipEntity) obj).id == id;
	}

	@Override
	public int hashCode() {
		return (int) id;
	}
}
