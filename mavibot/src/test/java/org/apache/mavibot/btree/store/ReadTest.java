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
package org.apache.mavibot.btree.store;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.junit.Test;


/**
 * Test the RecordManager.readXXX() methods using reflection
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class ReadTest
{
    /**
     * Test the readInt method
     */
    @Test
    public void testReadInt() throws Exception
    {
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        // Create page size of 32 only
        RecordManager recordManager = new RecordManager( tempFileName, 32 );
        Method storeMethod = RecordManager.class.getDeclaredMethod( "store", PageIO[].class, long.class, int.class );
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
        storeMethod.invoke( recordManager, pageIos, 0, 0x12345678 );

        // Read it back
        int readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 0 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page
        storeMethod.invoke( recordManager, pageIos, 16, 0x12345678 );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 16 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        storeMethod.invoke( recordManager, pageIos, 17, 0x12345678 );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 17 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 18, 0x12345678 );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 18 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 19, 0x12345678 );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 19 );

        assertEquals( 0x12345678, readValue );

        // Set the int at the beginning of the second page
        storeMethod.invoke( recordManager, pageIos, 20, 0x12345678 );

        // Read it back
        readValue = ( Integer ) readIntMethod.invoke( recordManager, pageIos, 20 );
    }


    /**
     * Test the readLong method
     */
    @Test
    public void testReadLong() throws Exception
    {
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        // Create page size of 32 only
        RecordManager recordManager = new RecordManager( tempFileName, 32 );
        Method storeMethod = RecordManager.class.getDeclaredMethod( "store", PageIO[].class, long.class, long.class );
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
        storeMethod.invoke( recordManager, pageIos, 0, 0x0123456789ABCDEFL );

        // Read it back
        long readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 0 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page
        storeMethod.invoke( recordManager, pageIos, 12, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 12 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        storeMethod.invoke( recordManager, pageIos, 13, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 13 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 14, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 14 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 15, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 15 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 4 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 16, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 16 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 5 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 17, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 17 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 6 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 18, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 18 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the end of the first page and overlapping on the second page
        // 7 bytes overlapping
        storeMethod.invoke( recordManager, pageIos, 19, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 19 );

        assertEquals( 0x0123456789ABCDEFL, readValue );

        // Set the int at the beginning of the second page
        storeMethod.invoke( recordManager, pageIos, 20, 0x0123456789ABCDEFL );

        // Read it back
        readValue = ( Long ) readLongMethod.invoke( recordManager, pageIos, 20 );
    }
}
