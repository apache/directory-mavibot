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


/**
 * A structure containing a Page on disk. It's a byte[PageSize] plus a few informations like
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
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class PageIO
{
    /** The contain data */
    private byte[] data;

    /** A pointer to the next pageIO */
    private long nextPage;

    /** The offset on disk */
    private long size;

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
    public byte[] getData()
    {
        return data;
    }


    /**
     * @param data the data to set
     */
    public void setData( byte[] data )
    {
        this.data = data;
    }


    /**
     * Get the NextPage value from the PageIO. If it's -1, there is no next page<br/>
     * @return the nextPage
     */
    public long getNextPage()
    {
        // read the nextPage from the PageIO
        nextPage = ( ( long ) data[0] << 56 ) +
            ( ( data[1] & 0xFFL ) << 48 ) +
            ( ( data[2] & 0xFFL ) << 40 ) +
            ( ( data[3] & 0xFFL ) << 32 ) +
            ( ( data[4] & 0xFFL ) << 24 ) +
            ( ( data[5] & 0xFFL ) << 16 ) +
            ( ( data[6] & 0xFFL ) << 8 ) +
            ( data[7] & 0xFFL );

        return nextPage;
    }


    /**
     * @param nextPage the nextPage to set
     */
    public void setNextPage( long nextPage )
    {
        this.nextPage = nextPage;

        data[0] = ( byte ) ( nextPage >>> 56 );
        data[1] = ( byte ) ( nextPage >>> 48 );
        data[2] = ( byte ) ( nextPage >>> 40 );
        data[3] = ( byte ) ( nextPage >>> 32 );
        data[4] = ( byte ) ( nextPage >>> 24 );
        data[5] = ( byte ) ( nextPage >>> 16 );
        data[6] = ( byte ) ( nextPage >>> 8 );
        data[7] = ( byte ) ( nextPage );
    }


    /**
     * @return the size
     */
    public long getSize()
    {
        size = ( data[8] << 24 ) +
            ( ( data[9] & 0xFF ) << 16 ) +
            ( ( data[10] & 0xFF ) << 8 ) +
            ( data[11] & 0xFF );

        return size;
    }


    /**
     * @param size the size to set
     */
    public void setSize( long size )
    {
        data[8] = ( byte ) ( size >>> 24 );
        data[9] = ( byte ) ( size >>> 16 );
        data[10] = ( byte ) ( size >>> 8 );
        data[11] = ( byte ) ( size );

        this.size = size;
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
}
