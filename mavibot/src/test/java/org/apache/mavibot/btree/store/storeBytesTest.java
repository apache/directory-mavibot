package org.apache.mavibot.btree.store;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.junit.Test;


public class storeBytesTest
{
    @Test
    public void testInjectIntoOnePage() throws Exception
    {
        File tempFile = File.createTempFile( "mavibot", ".db" );
        String tempFileName = tempFile.getAbsolutePath();
        tempFile.deleteOnExit();

        RecordManager recordManager = new RecordManager( tempFileName );
        Method method = RecordManager.class.getDeclaredMethod( "storeBytes", PageIO[].class, long.class, int.class );
        method.setAccessible( true );

        // Allocate some Pages
        PageIO[] pageIos = new PageIO[2];
        pageIos[0] = new PageIO();
        pageIos[0].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );
        pageIos[1] = new PageIO();
        pageIos[1].setData( ByteBuffer.allocate( recordManager.getPageSize() ) );

        // Set the int at the beginning
        long position = ( Long ) method.invoke( recordManager, pageIos, 0, 0x12345678 );

        assertEquals( 4, position );
        assertEquals( 0x12, pageIos[0].getData().get( 12 ) );
        assertEquals( 0x34, pageIos[0].getData().get( 13 ) );
        assertEquals( 0x56, pageIos[0].getData().get( 14 ) );
        assertEquals( 0x78, pageIos[0].getData().get( 15 ) );

        // Set the int at the end of the first page
        position = ( Long ) method.invoke( recordManager, pageIos, 4080, 0x12345678 );

        assertEquals( 4084, position );
        assertEquals( 0x12, pageIos[0].getData().get( 4092 ) );
        assertEquals( 0x34, pageIos[0].getData().get( 4093 ) );
        assertEquals( 0x56, pageIos[0].getData().get( 4094 ) );
        assertEquals( 0x78, pageIos[0].getData().get( 4095 ) );

        // Set the int at the end of the first page and overlapping on the second page
        // 1 byte overlapping
        position = ( Long ) method.invoke( recordManager, pageIos, 4081, 0x12345678 );

        assertEquals( 4085, position );
        assertEquals( 0x12, pageIos[0].getData().get( 4093 ) );
        assertEquals( 0x34, pageIos[0].getData().get( 4094 ) );
        assertEquals( 0x56, pageIos[0].getData().get( 4095 ) );
        assertEquals( 0x78, pageIos[1].getData().get( 8 ) );

        // Set the int at the end of the first page and overlapping on the second page
        // 2 bytes overlapping
        position = ( Long ) method.invoke( recordManager, pageIos, 4082, 0x12345678 );

        assertEquals( 4086, position );
        assertEquals( 0x12, pageIos[0].getData().get( 4094 ) );
        assertEquals( 0x34, pageIos[0].getData().get( 4095 ) );
        assertEquals( 0x56, pageIos[1].getData().get( 8 ) );
        assertEquals( 0x78, pageIos[1].getData().get( 9 ) );

        // Set the int at the end of the first page and overlapping on the second page
        // 3 bytes overlapping
        position = ( Long ) method.invoke( recordManager, pageIos, 4083, 0x12345678 );

        assertEquals( 4087, position );
        assertEquals( 0x12, pageIos[0].getData().get( 4095 ) );
        assertEquals( 0x34, pageIos[1].getData().get( 8 ) );
        assertEquals( 0x56, pageIos[1].getData().get( 9 ) );
        assertEquals( 0x78, pageIos[1].getData().get( 10 ) );

        // Set the int at the beginning of the second page
        position = ( Long ) method.invoke( recordManager, pageIos, 4084, 0x12345678 );

        assertEquals( 4088, position );
        assertEquals( 0x12, pageIos[1].getData().get( 8 ) );
        assertEquals( 0x34, pageIos[1].getData().get( 9 ) );
        assertEquals( 0x56, pageIos[1].getData().get( 10 ) );
        assertEquals( 0x78, pageIos[1].getData().get( 11 ) );
    }
}
