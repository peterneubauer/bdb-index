/**
 * Copyright (c) 2011 "Neo Technology,"
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

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
@Entity
class RelationshipEntity {

	@PrimaryKey
	long id;

	@SecondaryKey(relate=MANY_TO_ONE)//,relatedEntity=Node.class
	String key;

	@SecondaryKey(relate=MANY_TO_ONE)//,relatedEntity=Node.class
	String value;

	@SecondaryKey(relate=MANY_TO_ONE)//,relatedEntity=Node.class
	long sNodeId;

	@SecondaryKey(relate=MANY_TO_ONE)//,relatedEntity=Node.class
	long eNodeId;

	RelationshipEntity() {

	}

	RelationshipEntity(Relationship r, String k, String v) {

		id = r.getId();
		sNodeId = r.getStartNode().getId();
		eNodeId = r.getEndNode().getId();

		key = k;
		value = v;
	}

	public static RelationshipEntity of(Relationship r, String k, String v) {
		return new RelationshipEntity(r, k, v);
	}

	//	@Override
	//	public boolean equals( Object obj ) {
	//		RelationshipEntity entity = (RelationshipEntity) obj;
	//
	//		return entity.id == id && entity.key == key && entity.value == value;
	//	}
}
