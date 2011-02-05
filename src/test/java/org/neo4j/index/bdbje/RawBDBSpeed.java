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

import java.io.File;
import java.io.IOException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class RawBDBSpeed
{
    private static final String BDB = "target/bdb";
    private static final String DB_NAME = "testdb";
    private static Database bdb;

    public static void main( String[] args ) throws Exception
    {
        File bdbDir = new File( BDB );

        deleteFileOrDirectory( bdbDir );
        EnvironmentConfig environmentConfig = new EnvironmentConfig();
        environmentConfig.setAllowCreate( true );
        environmentConfig.setConfigParam( "java.util.logging.level", "INFO" );
        // perform other environment configurations
        Environment environment = new Environment( bdbDir, environmentConfig );
        environmentConfig.setTransactional( false );
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setAllowCreate( true );
        // perform other database configurations
        bdb = environment.openDatabase( null, DB_NAME, databaseConfig );

        long t = System.currentTimeMillis();
        for ( int i = 0; i < 5000000; i++ )
        {
            String key = "" + i;
            bdb.put( null, new DatabaseEntry( key.getBytes() ),
                    new DatabaseEntry( key.getBytes() ) );
            if ( i % 100000 == 0 )
            {
                System.out.print( "." );
            }
        }
        System.out.println( "insert time:" + ( System.currentTimeMillis() - t ) );

        t = System.currentTimeMillis();
        for ( int i = 0; i < 1000; i++ )
        {
            DatabaseEntry theData = new DatabaseEntry();
            OperationStatus operationStatus = bdb.get( null, new DatabaseEntry(
                    ( i + "" ).getBytes() ), theData, LockMode.READ_UNCOMMITTED );
            byte[] array = theData.getData();
            fastToLong( array );
        }
        System.out.println( "1000 lookups:" + ( System.currentTimeMillis() - t ) );

        bdb.close();
    }

    private static byte[] fastToBytes( long value ) throws IOException
    {
        byte[] array = new byte[8];
        for ( int i = 0; i < 8; i++ )
        {
            array[7 - i] = (byte) ( value >>> ( i * 8 ) );
        }
        return array;
    }

    private static long fastToLong( byte[] array ) throws IOException
    {
        long value = 0;
        for ( int i = 0; i < array.length; i++ )
        {
            value <<= 8;
            value ^= (long) array[i] & 0xFF;
        }
        return value;
    }

    private static byte[] lookupKey( String key, Object value )
    {
        return String.valueOf( key + "|" + value ).getBytes();
    }

    private static byte[] key( long id, String key, Object value )
    {
        return String.valueOf( key + "|" + value ).getBytes();
    }

    public static void deleteFileOrDirectory( File file )
    {
        if ( !file.exists() )
        {
            file.mkdir();
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
            file.delete();
        }
        else
        {
            file.delete();
        }
    }
}
