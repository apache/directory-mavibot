/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.apache.directory.mavibot.btree;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.exception.BTreeAlreadyManagedException;
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * test the RecordManager's free page management
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManagerFreePageTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager1 = null;

    private File dataDir = null;


    @Before
    public void createBTree() throws IOException
    {
        dataDir = new File( System.getProperty( "java.io.tmpdir" ) + "/recordman" );

        if ( dataDir.exists() )
        {
            FileUtils.deleteDirectory( dataDir );
        }

        dataDir.mkdirs();

        openRecordManagerAndBtree();

        try
        {
            // Create a new BTree
            btree = recordManager1.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }


    @After
    public void cleanup() throws IOException
    {
        dataDir = new File( System.getProperty( "java.io.tmpdir" ) + "/recordman" );

        btree.close();

        recordManager1.close();
        assertTrue( recordManager1.isContextOk() );

        if ( dataDir.exists() )
        {
            FileUtils.deleteDirectory( dataDir );
        }
    }


    private void openRecordManagerAndBtree()
    {
        try
        {
            if ( recordManager1 != null )
            {
                recordManager1.close();
            }

            // Now, try to reload the file back
            recordManager1 = new RecordManager( dataDir.getAbsolutePath() );

            // load the last created btree
            if ( btree != null )
            {
                btree = recordManager1.getManagedTree( btree.getName() );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private int nbElems = 10000;


    /**
     * Test the creation of a RecordManager, and that we can read it back.
     */
    @Test
    public void testRecordManager() throws IOException, BTreeAlreadyManagedException, KeyNotFoundException
    {
        assertEquals( 1, recordManager1.getNbManagedTrees() );

        Set<String> managedBTrees = recordManager1.getManagedTrees();

        assertEquals( 1, managedBTrees.size() );
        assertTrue( managedBTrees.contains( "test" ) );

        int nbError = 0;

        long l1 = System.currentTimeMillis();
        int n = 0;
        long delta = l1;

        for ( int i = 0; i < nbElems; i++ )
        {
            // System.out.println( i );
            Long key = ( long ) i;
            String value = Long.toString( key );

            btree.insert( key, value );

            if ( i % 10000 == 0 )
            {
                if ( n > 0 )
                {
                    long t0 = System.currentTimeMillis();
                    System.out.println( "Written " + i + " elements in : " + ( t0 - delta ) + "ms" );
                    delta = t0;
                }

                n++;
            }
        }

        long l2 = System.currentTimeMillis();

        System.out.println( "Delta : " + ( l2 - l1 ) + ", nbError = " + nbError
            + ", Nb insertion per second : " + ( ( nbElems ) / ( l2 - l1 ) ) * 1000 );

        long length = new File( dataDir, "mavibot.db" ).length();
        String units = "MB";

        long size = length / ( 1024 * 1024 );

        if ( size == 0 )
        {
            size = length / 1024;
            units = "KB";
        }

        // System.out.println( size + units );

        openRecordManagerAndBtree();

        assertEquals( 1, recordManager1.getNbManagedTrees() );

        assertTrue( nbElems == btree.getNbElems() );

        TupleCursor<Long, String> cursor = btree.browse();

        long i = 0;

        while ( cursor.hasNext() )
        {
            Tuple<Long, String> t = cursor.next();
            assertEquals( ( Long ) i, t.getKey() );
            assertEquals( String.valueOf( i ), t.getValue() );
            i++;
        }

        cursor.close();

        assertEquals( nbElems, i );
    }
}
