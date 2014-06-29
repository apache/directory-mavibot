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
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.apache.directory.mavibot.btree.PageIO;
import org.apache.directory.mavibot.btree.RecordManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test the RecordManager.readXXX() methods using reflection
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedReadTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();


    /**
     * Test the readInt method
     */
    @Test
    public void testReadInt() throws Exception
    {
        File tempFile = tempFolder.newFile( "mavibot.db" );
        String tempFileName = tempFile.getAbsolutePath();

        // Create page size of 32 only
        RecordManager recordManager = new RecordManager( tempFileName, 32 );
        Method storeMethod = RecordManager.class.getDeclaredMethod( "store", long.class, int.class, PageIO[].class );
        Method readIntMethod = RecordManager.class.getDeclaredMethod( "readInt", PageIO[].class, long.class );
        storeMethod.setAccessible( true );
        readIntMethod.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[2];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // Set the int at the beginning
        storeMethod.invoke( recordManager, 0, 0x12345678, pageIos );

        // Read it back
        int readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 0 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page
        storeMethod.invoke( recordManager, 16, 0x12345678, pageIos );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 16 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        storeMethod.invoke( recordManager, 17, 0x12345678, pageIos );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 17 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        storeMethod.invoke( recordManager, 18, 0x12345678, pageIos );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 18 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        storeMethod.invoke( recordManager, 19, 0x12345678, pageIos );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 19 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the beginning of the second page
        storeMethod.invoke( recordManager, 20, 0x12345678, pageIos );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 20 );

        recordManager.close();
    }


    /**
     * Test the readLong method
     */
    @Test
    public void testReadLong() throws Exception
    {
        File tempFile = tempFolder.newFile( "mavibot.db" );
        String tempFileName = tempFile.getAbsolutePath();

        // Create page size of 32 only
        RecordManager recordManager = new RecordManager( tempFileName, 32 );
        Method storeMethod = RecordManager.class.getDeclaredMethod( "store", long.class, long.class, PageIO[].class );
        Method readLongMethod = RecordManager.class.getDeclaredMethod( "readLong", PageIO[].class, long.class );
        storeMethod.setAccessible( true );
        readLongMethod.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[2];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // Set the int at the beginning
        storeMethod.invoke( recordManager, 0, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        long readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 0 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page
        storeMethod.invoke( recordManager, 12, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 12 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        storeMethod.invoke( recordManager, 13, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 13 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        storeMethod.invoke( recordManager, 14, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 14 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        storeMethod.invoke( recordManager, 15, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 15 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 4 bytes overlapping
        storeMethod.invoke( recordManager, 16, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 16 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 5 bytes overlapping
        storeMethod.invoke( recordManager, 17, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 17 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 6 bytes overlapping
        storeMethod.invoke( recordManager, 18, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 18 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 7 bytes overlapping
        storeMethod.invoke( recordManager, 19, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 19 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the beginning of the second page
        storeMethod.invoke( recordManager, 20, 0x0123456789ABCDEFL, pageIos );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 20 );

        recordManager.close();
    }


    /**
     * Test the readBytes() method
     */
    @Test
    public void testReadBytes() throws Exception
    {
        File tempFile = tempFolder.newFile( "mavibot.db" );
        String tempFileName = tempFile.getAbsolutePath();

        // We use smaller pages
        RecordManager recordManager = new RecordManager( tempFileName, 32 );
        Method storeMethod = RecordManager.class.getDeclaredMethod( "store", long.class, byte[].class, PageIO[].class );
        Method readBytesMethod = RecordManager.class.getDeclaredMethod( "readBytes", PageIO[].class, long.class );
        storeMethod.setAccessible( true );
        readBytesMethod.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[4];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[2] = new PageIO();
        pageIos[2].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[3] = new PageIO();
        pageIos[3].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // We start with 4 bytes
        byte[] bytes = new byte[]
            { 0x01, 0x23, 0x45, 0x67 };

        // Set the bytes at the beginning
        storeMethod.invoke( recordManager, 0L, bytes, pageIos );

        // Read the bytes back
        ByteBuffer readBytes = ( ByteBuffer ) readBytesMethod.invoke( recordManager, pageIos, 0L );

        // The byte length
        assertNotNull( readBytes );
        assertEquals( 4, readBytes.limit() );
        // The data
        assertEquals( 0x01, readBytes.get() );
        assertEquals( 0x23, readBytes.get() );
        assertEquals( 0x45, readBytes.get() );
        assertEquals( 0x67, readBytes.get() );

        // Set the bytes at the end of the first page
        storeMethod.invoke( recordManager, 12L, bytes, pageIos );

        // Read the bytes back
        readBytes = ( ByteBuffer ) readBytesMethod.invoke( recordManager, pageIos, 12L );

        // The byte length
        assertNotNull( readBytes );
        assertEquals( 4, readBytes.limit() );
        // The data
        assertEquals( 0x01, readBytes.get() );
        assertEquals( 0x23, readBytes.get() );
        assertEquals( 0x45, readBytes.get() );
        assertEquals( 0x67, readBytes.get() );

        // Set A full page of bytes in the first page 
        bytes = new byte[16];

        for ( int i = 0; i < 16; i++ )
        {
            bytes[i] = ( byte ) ( i + 1 );
        }

        storeMethod.invoke( recordManager, 0L, bytes, pageIos );

        // Read the bytes back
        readBytes = ( ByteBuffer ) readBytesMethod.invoke( recordManager, pageIos, 0L );

        // The byte length
        assertNotNull( readBytes );
        assertEquals( 16, readBytes.limit() );

        // The data
        for ( int i = 0; i < 16; i++ )
        {
            assertEquals( i + 1, readBytes.get() );
        }

        // Write the bytes over 2 pages
        storeMethod.invoke( recordManager, 15L, bytes, pageIos );

        // Read the bytes back
        readBytes = ( ByteBuffer ) readBytesMethod.invoke( recordManager, pageIos, 15L );

        // The byte length
        assertNotNull( readBytes );
        assertEquals( 16, readBytes.limit() );
        // The data
        for ( int i = 0; i < 16; i++ )
        {
            assertEquals( i + 1, readBytes.get() );
        }

        // Write the bytes over 4 pages
        bytes = new byte[80];

        for ( int i = 0; i < 80; i++ )
        {
            bytes[i] = ( byte ) ( i + 1 );
        }

        storeMethod.invoke( recordManager, 2L, bytes, pageIos );

        // Read the bytes back
        readBytes = ( ByteBuffer ) readBytesMethod.invoke( recordManager, pageIos, 2L );

        // The byte length
        assertNotNull( readBytes );
        assertEquals( 80, readBytes.limit() );

        // The data
        for ( int i = 0; i < 80; i++ )
        {
            assertEquals( i + 1, readBytes.get() );
        }

        recordManager.close();
    }
}
