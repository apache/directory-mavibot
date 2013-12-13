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
package org.apache.directory.mavibot.btree.persisted;


import java.nio.ByteBuffer;

import org.apache.directory.mavibot.btree.util.Strings;


/**
 * A structure containing a Page on disk. It's a byte[PageSize] plus a few more details like
 * the page offset on disk and a link to the next page.</br>
 * As we may need more than one Page to store some data, the PageIO are linked so that
 * the list of all the PageIO contain the full data.</br>
 * The first PageIO contains the size of the data.</br>
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

    /** The position of the page on disk */
    private long offset;


    /** 
     * A default constructor for a PageIO
     */
    public PageIO()
    {
        nextPage = -2L;
        size = -1;
        offset = -1L;
    }


    /** 
     * A constructor for a PageIO when we know the offset of this page on disk
     */
    public PageIO( long offset )
    {
        nextPage = -2L;
        size = -1;
        this.offset = offset;
    }


    /**
     * @return the data
     */
    public ByteBuffer getData()
    {
        return data;
    }


    /**
     * @param data the data to set
     */
    public void setData( ByteBuffer data )
    {
        this.data = data;
        nextPage = data.getLong( 0 );
    }


    /**
     * Get the NextPage value from the PageIO. If it's -1, there is no next page<br/>
     * @return the nextPage
     */
    public long getNextPage()
    {
        return nextPage;
    }


    /**
     * @param nextPage the nextPage to set
     */
    public void setNextPage( long nextPage )
    {
        this.nextPage = nextPage;

        data.putLong( 0, nextPage );
    }


    /**
     * @return the size
     */
    public long getSize()
    {
        return size;
    }


    /**
     * @param size the size to set
     */
    public void setSize( int size )
    {
        data.putInt( 8, size );

        this.size = size;
    }


    /**
     * @param size the size to set
     */
    public void setSize()
    {
        size = data.getInt( 8 );
    }


    /**
     * @return the offset
     */
    public long getOffset()
    {
        return offset;
    }


    /**
     * @param offset the offset to set
     */
    public void setOffset( long offset )
    {
        this.offset = offset;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "PageIO[offset:" ).append( offset );

        if ( size != -1 )
        {
            sb.append( ", size:" ).append( size );
        }

        if ( nextPage != -1L )
        {
            sb.append( ", next:" ).append( nextPage );
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
