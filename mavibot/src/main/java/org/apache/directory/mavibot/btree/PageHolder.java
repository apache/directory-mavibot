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

import java.io.IOException;

import org.apache.commons.collections.map.LRUMap;
import org.apache.directory.mavibot.btree.exception.BTreeOperationException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;

/**
 * A Page holder. It stores the page and provide a way to access it.
 * 
 * @param <K> The type of the BTree key
 * @param <V> The type of the BTree value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class PageHolder<K, V>
{
    /** The BTree */
    protected BTree<K, V> btree;

    /** The RecordManager */
    private RecordManager recordManager;

    /** The RecordManagerHeader */
    private RecordManagerHeader recordManagerHeader;

    /** The cache */
    private LRUMap cache;

    /** The loaded page */
    private Page<K, V> page;


    /**
     * Create a new holder storing an offset and a SoftReference containing the element.
     * 
     * @param btree The associated BTree
     * @param page The element to store into a SoftReference
     **/
    public PageHolder( BTree<K, V> btree, Page<K, V> page )
    {
        this.btree = btree;

        //cache = ( ( BTreeImpl<K, V> ) btree ).getCache();
        recordManager = btree.getRecordManager();
        recordManagerHeader = btree.getRecordManagerHeader();

        //cache.put( offset, page );
        this.page = page;
    }


    /**
     * {@inheritDoc}
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public Page<K, V> getValue()
    {
        //Page<K, V> page = ( Page<K, V> ) cache.get( offset );

        if ( page == null )
        {
            // We have to fetch the element from disk, using the offset now
            page = fetchElement();

            //cache.put( offset, page );
        }

        return page;
    }


    /**
     * Retrieve the value from the disk, using the BTree and offset
     * @return The deserialized element (
     */
    private Page<K, V> fetchElement()
    {
        try
        {
            return recordManager.deserialize( recordManagerHeader, btree, getOffset() );
        }
        catch ( EndOfFileExceededException eofee )
        {
            throw new BTreeOperationException( eofee.getMessage(), eofee );
        }
        catch ( IOException ioe )
        {
            throw new BTreeOperationException( ioe.getMessage(), ioe );
        }
    }


    /**
     * @return The offset of the first {@link PageIO} storing the data on disk
     */
    /* No qualifier */long getOffset()
    {
        return page.getOffset();
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        Page<K, V> page = getValue();

        sb.append( btree.getName() ).append( "[" ).append( getOffset() ).append( "]:" ).append( page );

        return sb.toString();
    }
}
