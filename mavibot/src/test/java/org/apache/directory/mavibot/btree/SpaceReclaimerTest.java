/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */
package org.apache.directory.mavibot.btree;


import java.io.File;

import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.*;

/**
 * Tests for free page reclaimer.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class SpaceReclaimerTest
{
    private static final String TREE_NAME = "uid-tree";

    private RecordManager rm;

    private PersistedBTree<Integer, String> uidTree;
    
    @Rule
    public TemporaryFolder tmpDir;

    private File dbFile;


    @Before
    public void setup() throws Exception
    {
        tmpDir = new TemporaryFolder();
        tmpDir.create();
        
        dbFile = tmpDir.newFile( "spacereclaimer.db" );

        System.out.println(dbFile.getAbsolutePath());
        rm = new RecordManager( dbFile.getAbsolutePath() );
        rm.setSpaceReclaimerThreshold( 10 );
        
        uidTree = ( PersistedBTree<Integer, String> ) rm.addBTree( TREE_NAME, IntSerializer.INSTANCE, StringSerializer.INSTANCE, false );
    }


    @After
    public void cleanup() throws Exception
    {
        rm.close();
        dbFile.delete();
        tmpDir.delete();
    }

    
    private void closeAndReopenRM() throws Exception
    {
        uidTree.close();
        rm.close();
        rm = new RecordManager( dbFile.getAbsolutePath() );
        uidTree = ( PersistedBTree ) rm.getManagedTree( TREE_NAME );
    }

    
    @Test
    public void testReclaimer() throws Exception
    {
        int total = 11;
        System.out.println( dbFile.length() );
        for ( int i=0; i < total; i++ )
        {
            uidTree.insert( i, String.valueOf( i ) );
        }

        System.out.println( "Total size before closing " + dbFile.length() );
        closeAndReopenRM();
        
        System.out.println( "Total size AFTER closing " + dbFile.length() );
        
        int count = 0;
        TupleCursor<Integer, String> cursor = uidTree.browse();
        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> t = cursor.next();
            assertEquals( t.key, Integer.valueOf( count ) );
            count++;
        }
        
        assertEquals( count, total );
    }
    

    /**
     * with the reclaimer threshold 10 and total entries of 1120
     * there was a condition that resulted in OOM while reopening the RM
     * 
     * This issue was fixed after SpaceReclaimer was updated to run in
     * a transaction.
     * 
     * This test is present to verify the fix
     * 
     * @throws Exception
     */
    @Test
    public void testReclaimerWithMagicNum() throws Exception
    {
    	rm.setSpaceReclaimerThreshold( 10 );
    	
        int total = 1120;
        for ( int i=0; i < total; i++ )
        {
            uidTree.insert( i, String.valueOf( i ) );
        }

        closeAndReopenRM();
        
        int count = 0;
        TupleCursor<Integer, String> cursor = uidTree.browse();
        while ( cursor.hasNext() )
        {
            Tuple<Integer, String> t = cursor.next();
            assertEquals( t.key, Integer.valueOf( count ) );
            count++;
        }
        
        assertEquals( count, total );
    }
}
