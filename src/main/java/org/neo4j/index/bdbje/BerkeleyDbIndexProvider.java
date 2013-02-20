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

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConnectionBroker;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.index.ReadOnlyIndexConnectionBroker;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;

@Service.Implementation( IndexProvider.class )
public final class BerkeleyDbIndexProvider extends IndexProvider
{

	public BerkeleyDbIndexProvider()
	{
		super( BerkeleyDbIndexImplementation.SERVICE_NAME );
	}

	@Override
	public IndexImplementation load( DependencyResolver dependencyResolver ) throws Exception
	{
		Config config = dependencyResolver.resolveDependency(Config.class);
		AbstractGraphDatabase gdb = dependencyResolver.resolveDependency(AbstractGraphDatabase.class);
		TransactionManager txManager = dependencyResolver.resolveDependency(TransactionManager.class);
		IndexStore indexStore = dependencyResolver.resolveDependency(IndexStore.class);
		XaFactory xaFactory = dependencyResolver.resolveDependency(XaFactory.class);
		FileSystemAbstraction fileSystemAbstraction = dependencyResolver.resolveDependency(FileSystemAbstraction.class);
		XaDataSourceManager xaDataSourceManager = dependencyResolver.resolveDependency(XaDataSourceManager.class);

		BerkeleyDbDataSource dataSource =
				new BerkeleyDbDataSource(
						config,
						indexStore,
						fileSystemAbstraction,
						xaFactory
						);

		xaDataSourceManager.registerDataSource(dataSource);

		IndexConnectionBroker<BerkeleyDbXaConnection> broker =
				dataSource.isReadOnly()
				? new ReadOnlyIndexConnectionBroker<BerkeleyDbXaConnection>( txManager )
						: new ConnectionBroker( txManager, dataSource );

				return new BerkeleyDbIndexImplementation( gdb, dataSource, broker );
	}
}
