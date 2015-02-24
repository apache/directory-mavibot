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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests for KeyCursor of a persisted sub-Btree.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedSubBtreeKeyCursorTest
{
    private BTree<Integer, Integer> btree = null;

    private RecordManager recordManager = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    @Before
    public void createBTree() throws IOException
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        // Now, try to reload the file back
        recordManager = new RecordManager( dataDir.getAbsolutePath() );

        try
        {
            PersistedBTreeConfiguration<Integer, Integer> configuration = new PersistedBTreeConfiguration<Integer, Integer>();
            configuration.setAllowDuplicates( false );
            configuration.setKeySerializer( IntSerializer.INSTANCE );
            configuration.setValueSerializer( IntSerializer.INSTANCE );
            configuration.setName( "sub-btree" );
            configuration.setBtreeType( BTreeTypeEnum.PERSISTED_SUB );

            btree = BTreeFactory.createPersistedBTree( configuration );

            recordManager.manage( btree );
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

        if ( dataDir.exists() )
        {
            FileUtils.deleteDirectory( dataDir );
        }

        recordManager.close();
        assertTrue( recordManager.isContextOk() );
    }


    @Test
    public void testBrowseKeys() throws Exception
    {
        for ( int i = 0; i < 10; i++ )
        {
            // only the keys are stored, values are ignored
            btree.insert( i, i );
        }

        KeyCursor<Integer> cursor = btree.browseKeys();

        for ( int i = 0; i < 10; i++ )
        {
            assertTrue( cursor.hasNext() );
            assertEquals( String.valueOf( i ), String.valueOf( cursor.next() ) );
        }

        assertFalse( cursor.hasNext() );

        cursor.afterLast();

        for ( int i = 9; i >= 0; i-- )
        {
            assertTrue( cursor.hasPrev() );
            assertEquals( String.valueOf( i ), String.valueOf( cursor.prev() ) );
        }

        assertFalse( cursor.hasPrev() );
        cursor.close();
    }
}
