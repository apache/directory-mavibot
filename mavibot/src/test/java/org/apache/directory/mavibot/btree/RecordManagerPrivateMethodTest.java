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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;

import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test some of the RecordManager prvate methods
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RecordManagerPrivateMethodTest
{
    private BTree<Long, String> btree = null;

    private RecordManager recordManager = null;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File dataDir = null;


    @Before
    public void createRecordManager() throws Exception
    {
        dataDir = tempFolder.newFolder( UUID.randomUUID().toString() );

        System.out.println( dataDir + "/mavibot.db" );

        // Now, try to reload the file back
        recordManager = new RecordManager( dataDir.getAbsolutePath(), 32 );

        // Create a new BTree
        btree = recordManager.addBTree( "test", LongSerializer.INSTANCE, StringSerializer.INSTANCE, false );
    }


    @After
    public void closeBTree() throws IOException
    {
        recordManager.close();
    }


    /**
     * Test the getFreePageIOs method
     */
    @Test
    public void testGetFreePageIos() throws IOException, NoSuchMethodException, InvocationTargetException,
        IllegalAccessException
    {
        Method getFreePageIOsMethod = RecordManager.class.getDeclaredMethod( "getFreePageIOs", int.class );
        getFreePageIOsMethod.setAccessible( true );

        PageIO[] pages = ( org.apache.directory.mavibot.btree.PageIO[] ) getFreePageIOsMethod.invoke( recordManager, 0 );

        assertEquals( 0, pages.length );

        for ( int i = 1; i <= 52; i++ )
        {
            pages = ( org.apache.directory.mavibot.btree.PageIO[] ) getFreePageIOsMethod.invoke( recordManager, i );
            assertEquals( 1, pages.length );
        }

        for ( int i = 53; i <= 108; i++ )
        {
            pages = ( org.apache.directory.mavibot.btree.PageIO[] ) getFreePageIOsMethod.invoke( recordManager, i );
            assertEquals( 2, pages.length );
        }

        for ( int i = 109; i <= 164; i++ )
        {
            pages = ( org.apache.directory.mavibot.btree.PageIO[] ) getFreePageIOsMethod.invoke( recordManager, i );
            assertEquals( 3, pages.length );
        }

        btree.close();
    }


    /**
     * Test the ComputeNbPages method
     */
    @Test
    public void testComputeNbPages() throws IOException, SecurityException, NoSuchMethodException,
        IllegalArgumentException, IllegalAccessException, InvocationTargetException
    {
        Method computeNbPagesMethod = RecordManager.class.getDeclaredMethod( "computeNbPages", int.class );
        computeNbPagesMethod.setAccessible( true );

        assertEquals( 0, ( ( Integer ) computeNbPagesMethod.invoke( recordManager, 0 ) ).intValue() );

        for ( int i = 1; i < 53; i++ )
        {
            assertEquals( 1, ( ( Integer ) computeNbPagesMethod.invoke( recordManager, i ) ).intValue() );
        }

        for ( int i = 53; i < 109; i++ )
        {
            assertEquals( 2, ( ( Integer ) computeNbPagesMethod.invoke( recordManager, i ) ).intValue() );
        }

        for ( int i = 109; i < 164; i++ )
        {
            assertEquals( 3, ( ( Integer ) computeNbPagesMethod.invoke( recordManager, i ) ).intValue() );
        }

        btree.close();
    }
}
