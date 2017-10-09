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


import java.nio.ByteBuffer;

import org.apache.directory.mavibot.btree.util.Strings;


/**
 * A structure containing a Page on disk. It's a byte[PageSize] plus a few more details like
 * the data size (for the first PageIO) and a link to the next page, plus the offset on disk for itself.
 * <p>
 * The tricky part is that a PageIO is really just a fixed size byte[] read from the disk, and there is no
 * semantic associated to the PageIO : it's totally opaque. The semantic is added by the caller, which 
 * will decode the data to extract the added information from the data. The caller knows what to do with 
 * the contained data, the PageIO does not.
 * <p>
 * Typically : 
 * <ul>
 *   <li>The first PageIO contains the size of the data starting at position 8.</li>
 *   <li>Each PageIO first 8 bytes are the offset of teh next page, if any, or -1 if there is none</li>
 * </ul
 * >
 * Here is the logical structure of a PageIO :
 * <pre>
 * For a first page :
 *
 * +----------+------+----------------------+
 * | nextPage | size | XXXXXXXXXXXXXXXXXXXX |
 * +----------+------+----------------------+
 *
 * for any page but the first :
 *
 * +----------+-----------------------------+
 * | nextPage | XXXXXXXXXXXXXXXXXXXXXXXXXXX |
 * +----------+-----------------------------+
 *
 * for the last page :
 * +----------+-----------------------------+
 * |    -1    | XXXXXXXXXXXXXXXXXXXXXXXXXXX |
 * +----------+-----------------------------+
 *
 * In any case, the page length is always PageSize.
 * </pre>
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class PageIO
{
    /** The contain data */
    private ByteBuffer data;

    /** A pointer to the next pageIO */
    private long nextPage;

    /** The offset on disk */
    private int size;

    /** The offset on disk */
    private long offset;


    /**
     * A default constructor for a PageIO (used by tests)
     */
    /* no qualifier */PageIO()
    {
        nextPage = -2L;
        size = -1;
        offset = -1L;
    }


    /**
     * A constructor for a PageIO when we know the offset of this page on disk
     */
    /* no qualifier */PageIO( long offset )
    {
        nextPage = -2L;
        size = -1;
        this.offset = offset;
    }


    /**
     * @return the data
     */
    /* no qualifier */ByteBuffer getData()
    {
        return data;
    }


    /**
     * @param data the data to set
     */
    /* no qualifier */void setData( ByteBuffer data )
    {
        this.data = data;
        nextPage = data.getLong( 0 );
    }


    /**
     * Get the NextPage value from the PageIO. If it's -1, there is no next page<br/>
     * @return the nextPage
     */
    /* no qualifier */long getNextPage()
    {
        return nextPage;
    }


    /**
     * @param nextPage the nextPage to set
     */
    /* no qualifier */void setNextPage( long nextPage )
    {
        this.nextPage = nextPage;

        data.putLong( 0, nextPage );
    }


    /**
     * @return the size
     */
    /* no qualifier */long getSize()
    {
        return size;
    }


    /**
     * @param size the size to set
     */
    /* no qualifier */void setSize( int size )
    {
        data.putInt( 8, size );

        this.size = size;
    }


    /**
     * The default size
     */
    /* no qualifier */void setSize()
    {
        size = data.getInt( 8 );
    }


    /**
     * @return the offset
     */
    /* no qualifier */long getOffset()
    {
        return offset;
    }


    /**
     * @param offset the offset to set
     */
    /* no qualifier */void setOffset( long offset )
    {
        this.offset = offset;
    }


    /* no qualifier */PageIO copy( PageIO copy )
    {
        // The data
        if ( data.isDirect() )
        {
            copy.data = ByteBuffer.allocateDirect( data.capacity() );
        }
        else
        {
            copy.data = ByteBuffer.allocate( data.capacity() );
        }

        // Save the original buffer position and limit
        int start = data.position();
        int limit = data.limit();

        // The data is extended to get all the bytes in it
        data.position( 0 );
        data.limit( data.capacity() );

        // Copy the data
        copy.data.put( data );

        // Restore the original buffer to the initial position and limit
        data.position( start );
        data.limit( limit );

        // Set those position and limit in the copied buffer
        copy.data.position( start );
        copy.data.limit( limit );

        // The size
        copy.size = size;

        // The offset and next page pointers are not copied.
        return copy;
    }

    
    /**
     * Reads all the data from a set of PageIOs. The first PageIO contains the data length
     * @param pageIos The PageIO to load into the buffer
     * @return A ByteBuffer containing the data
     */
    /* No qualifier */static ByteBuffer getBuffer( PageIO[] pageIos )
    {
        if ( ( pageIos == null ) || ( pageIos.length ==0 ) )
        {
            return ByteBuffer.allocate( 0 );
        }
        
        ByteBuffer buffer = ByteBuffer.allocate( ( int ) pageIos[0].getSize() );
        
        boolean isFirst = true;
        
        for ( PageIO pageIo : pageIos )
        {
            ByteBuffer data = pageIo.getData();
            
            if ( isFirst )
            {
                data.position( BTreeConstants.LONG_SIZE  + BTreeConstants.INT_SIZE  );
                isFirst = false;
            }
            else
            {
                data.position( BTreeConstants.LONG_SIZE  );
            }
            
            buffer.put( data );
        }
        
        buffer.flip();
        
        return buffer;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "PageIO[offset:0x" ).append( Long.toHexString( offset ) );

        if ( size != -1 )
        {
            sb.append( ", size:" ).append( size );
        }

        if ( nextPage != -1L )
        {
            sb.append( ", next:0x" ).append( Long.toHexString( nextPage ) );
        }

        sb.append( "]" );

        int start = 0;

        byte[] array = null;

        data.mark();
        data.position( 0 );

        if ( data.isDirect() )
        {
            array = new byte[data.capacity()];
            data.get( array );
        }
        else
        {
            array = data.array();
        }

        data.reset();

        for ( int i = start; i < array.length; i++ )
        {
            if ( ( ( i - start ) % 16 ) == 0 )
            {
                sb.append( "\n    " );
            }

            sb.append( Strings.dumpByte( array[i] ) ).append( " " );
        }

        return sb.toString();
    }
}
