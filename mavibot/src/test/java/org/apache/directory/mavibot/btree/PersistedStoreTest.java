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
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test the RecordManager.store() method using reflection
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class PersistedStoreTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();


    /**
     * Test the store( int ) method
     */
    @Test
    public void testInjectInt() throws Exception
    {
        File tempFile = tempFolder.newFile( "mavibot.db" );
        String tempFileName = tempFile.getAbsolutePath();

        RecordManager recordManager = new RecordManager( tempFileName, 4 * 1024 );
        Method method = RecordManager.class.getDeclaredMethod( "store", long.class, int.class, PageIO[].class );
        method.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[2];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // Set the int at the beginning
        long position = ( Long ) method.invoke( recordManager, 0, 0x12345678, pageIos );

        assertEquals( 4, position );
        int pos = 12;
        assertEquals( 0x12, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x34, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x56, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x78, pageIos[0].getData().get( pos++ ) );

        // Set the int at the end of the first page
        position = ( Long ) method.invoke( recordManager, 4080, 0x12345678, pageIos );

        assertEquals( 4084, position );
        pos = 4092;
        assertEquals( 0x12, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x34, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x56, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x78, pageIos[0].getData().get( pos++ ) );

        // Set the int at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        position = ( Long ) method.invoke( recordManager, 4081, 0x12345678, pageIos );

        assertEquals( 4085, position );
        pos = 4093;
        assertEquals( 0x12, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x34, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x56, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( 0x78, pageIos[1].getData().get( pos++ ) );

        // Set the int at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4082, 0x12345678, pageIos );

        assertEquals( 4086, position );
        pos = 4094;
        assertEquals( 0x12, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x34, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( 0x56, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x78, pageIos[1].getData().get( pos++ ) );

        // Set the int at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4083, 0x12345678, pageIos );

        assertEquals( 4087, position );
        pos = 4095;
        assertEquals( 0x12, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( 0x34, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x56, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x78, pageIos[1].getData().get( pos++ ) );

        // Set the int at the beginning of the second page
        position = ( Long ) method.invoke( recordManager, 4084, 0x12345678, pageIos );

        assertEquals( 4088, position );
        pos = 8;
        assertEquals( 0x12, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x34, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x56, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x78, pageIos[1].getData().get( pos++ ) );

        recordManager.close();
    }


    /**
     * Test the store( long ) method
     */
    @Test
    public void testInjectLong() throws Exception
    {
        File tempFile = tempFolder.newFile( "mavibot.db" );
        String tempFileName = tempFile.getAbsolutePath();

        RecordManager recordManager = new RecordManager( tempFileName, 4 * 1024 );
        Method method = RecordManager.class.getDeclaredMethod( "store", long.class, long.class, PageIO[].class );
        method.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[2];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // Set the long at the beginning
        long position = ( Long ) method.invoke( recordManager, 0, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 8, position );
        int pos = 12;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[0].getData().get( pos++ ) );

        // Set the long at the end of the first page
        position = ( Long ) method.invoke( recordManager, 4076, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4084, position );
        pos = 4088;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[0].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        position = ( Long ) method.invoke( recordManager, 4077, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4085, position );
        pos = 4089;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4078, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4086, position );
        pos = 4090;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4079, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4087, position );
        pos = 4091;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( ( byte ) 0xAB, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 4 byte overlapping
        position = ( Long ) method.invoke( recordManager, 4080, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4088, position );
        pos = 4092;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( ( byte ) 0x89, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 5 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4081, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4089, position );
        pos = 4093;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( 0x67, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 6 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4082, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4090, position );
        pos = 4094;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( 0x45, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the end of the first page and overlapping on the second page
        // 7 bytes overlapping
        position = ( Long ) method.invoke( recordManager, 4083, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4091, position );
        pos = 4095;
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        pos = 8;
        assertEquals( 0x23, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        // Set the long at the beginning of the second page
        position = ( Long ) method.invoke( recordManager, 4084, 0x0123456789ABCDEFL, pageIos );

        assertEquals( 4092, position );
        pos = 8;
        assertEquals( 0x01, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[1].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0x89, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xAB, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xCD, pageIos[1].getData().get( pos++ ) );
        assertEquals( ( byte ) 0xEF, pageIos[1].getData().get( pos++ ) );

        recordManager.close();
    }


    /**
     * Test the store( bytes ) method
     */
    @Test
    public void testInjectBytes() throws Exception
    {
        File tempFile = tempFolder.newFile( "mavibot.db" );
        String tempFileName = tempFile.getAbsolutePath();

        // We use smaller pages
        RecordManager recordManager = new RecordManager( tempFileName, 32 );
        Method storeMethod = RecordManager.class.getDeclaredMethod( "store", long.class, byte[].class, PageIO[].class );
        storeMethod.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[3];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[2] = new PageIO();
        pageIos[2].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
//        pageIos[3] = new PageIO();
//        pageIos[3].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // We start with 4 bytes
        byte[] bytes = new byte[]
            { 0x01, 0x23, 0x45, 0x67 };

        // Set the bytes at the beginning
        long position = ( Long ) storeMethod.invoke( recordManager, 0L, bytes, pageIos );

        assertEquals( 8, position );
        int pos = 12;
        // The byte length
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x04, pageIos[0].getData().get( pos++ ) );
        // The data
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );

        // Set the bytes at the end of the first page
        position = ( Long ) storeMethod.invoke( recordManager, 12L, bytes, pageIos );

        assertEquals( 20, position );
        pos = 24;
        // The byte length
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x04, pageIos[0].getData().get( pos++ ) );
        // The data
        assertEquals( 0x01, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x23, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x45, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x67, pageIos[0].getData().get( pos++ ) );

        // Set A full page of bytes in the first page
        bytes = new byte[16];

        for ( int i = 0; i < 16; i++ )
        {
            bytes[i] = ( byte ) ( i + 1 );
        }

        position = ( Long ) storeMethod.invoke( recordManager, 0L, bytes, pageIos );

        assertEquals( 20, position );
        pos = 12;
        // The byte length
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x10, pageIos[0].getData().get( pos++ ) );

        // The data
        for ( int i = 0; i < 16; i++ )
        {
            assertEquals( ( byte ) ( i + 1 ), pageIos[0].getData().get( pos++ ) );
        }

        // Write the bytes over 2 pages
        position = ( Long ) storeMethod.invoke( recordManager, 47L, bytes, pageIos );

        assertEquals( 67, position );
        pos = 59;
        // The byte length
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x10, pageIos[0].getData().get( pos++ ) );

        // The data in the first page
        assertEquals( 1, pageIos[0].getData().get( pos++ ) );

        // and in the second page
        pos = 8;

        for ( int i = 0; i < 15; i++ )
        {
            assertEquals( ( byte ) ( i + 2 ), pageIos[1].getData().get( pos++ ) );
        }

        // Write the bytes over 4 pages
        bytes = new byte[112];

        for ( int i = 0; i < 112; i++ )
        {
            bytes[i] = ( byte ) ( i + 1 );
        }

        position = ( Long ) storeMethod.invoke( recordManager, 2L, bytes, pageIos );

        assertEquals( 118, position );
        pos = 14;
        // The byte length
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x00, pageIos[0].getData().get( pos++ ) );
        assertEquals( 0x70, pageIos[0].getData().get( pos++ ) );

        // The data in the first page
        for ( int i = 0; i < 46; i++ )
        {
            assertEquals( ( byte ) ( i + 1 ), pageIos[0].getData().get( pos++ ) );
        }

        // The data in the second page
        pos = 8;

        for ( int i = 46; i < 102; i++ )
        {
            assertEquals( ( byte ) ( i + 1 ), pageIos[1].getData().get( pos++ ) );
        }

        // The data in the third page
        pos = 8;

        for ( int i = 102; i < 112; i++ )
        {
            assertEquals( ( byte ) ( i + 1 ), pageIos[2].getData().get( pos++ ) );
        }

        recordManager.close();
    }
}
