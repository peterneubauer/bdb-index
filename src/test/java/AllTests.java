/**
 * Copyright 2005-2011 cyuczieekc <Y3l1Y3ppZWVrYyBnbWFpbCBjb20K>
 * This file is part of neo4john.
 * 
 * neo4john is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * neo4john is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with neo4john. If not, see <http://www.gnu.org/licenses/>.
 */
import org.junit.runner.*;
import org.junit.runners.*;
import org.neo4j.index.bdbje.TestBerkeley;
import org.neo4j.index.bdbje.TestBerkeleyBatchInsert;



@RunWith( Suite.class )
@Suite.SuiteClasses(
		value = {
			TestBerkeley.class,
			TestBerkeleyBatchInsert.class
		} )
public class AllTests {
	// always empty
}
