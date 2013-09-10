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

import net.sf.ehcache.Element;

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A Value holder. As we may not store all the values in memory (except for an in-memory
 * BTree), we will use a SoftReference to keep a reference to a Value, and if it's null,
 * then we will load the Value from the underlying physical support, using the offset. 
 * 
 * @param <E> The type for the stored element (either a value or a page)
 * @param <K> The type of the BTree key
 * @param <V> The type of the BTree value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class CacheHolder<E, K, V> implements ElementHolder<E, K, V>
{
    /** The BTree */
    private BTree<K, V> btree;

    /** The offset of the first {@link PageIO} storing the page on disk */
    private long offset;

    /** The offset of the last {@link PageIO} storing the page on disk */
    private long lastOffset;


    /**
     * Create a new holder storing an offset and a SoftReference containing the element.
     * 
     * @param offset The offset in disk for this value
     * @param element The element to store into a SoftReference
     */
    public CacheHolder( BTree<K, V> btree, Page<K, V> element, long offset, long lastOffset )
    {
        this.btree = btree;
        this.offset = offset;
        this.lastOffset = lastOffset;

        if ( element instanceof Page<?, ?> )
        {
            ( ( AbstractPage<K, V> ) element ).setOffset( offset );
            ( ( AbstractPage<K, V> ) element ).setLastOffset( lastOffset );
        }

        btree.getCache().put( new Element( offset, element ) );
    }


    /**
     * {@inheritDoc}
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    @Override
    public E getValue( BTree<K, V> btree ) throws EndOfFileExceededException, IOException
    {
        Element element = btree.getCache().get( offset );

        if ( element == null )
        {
            // We haven't found the element in the cache, reload it
            // We have to fetch the element from disk, using the offset now
            Page<K, V> page = fetchElement( btree );

            btree.getCache().put( new Element( offset, page ) );

            return ( E ) page;
        }

        V value = ( V ) element.getObjectValue();

        if ( value == null )
        {
            // We have to fetch the element from disk, using the offset now
            Page<K, V> page = fetchElement( btree );

            if ( page instanceof Page<?, ?> )
            {
                ( ( AbstractPage<K, V> ) page ).setOffset( offset );
                ( ( AbstractPage<K, V> ) page ).setLastOffset( lastOffset );
            }

            btree.getCache().put( new Element( offset, page ) );

            element = btree.getCache().get( offset );

            return ( E ) page;
        }
        else
        {
            return ( E ) value;
        }
    }


    /**
     * Retrieve the value from the disk, using the BTree and offset
     * @return The deserialized element (
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private Page<K, V> fetchElement( BTree<K, V> btree ) throws EndOfFileExceededException, IOException
    {
        Page<K, V> element = btree.getRecordManager().deserialize( btree, offset );

        return element;
    }


    /**
     * @return The offset of the first {@link PageIO} storing the data on disk
     */
    /* No qualifier */long getOffset()
    {
        return offset;
    }


    /**
     * @return The offset of the last {@link PageIO} storing the data on disk
     */
    /* No qualifier */long getLastOffset()
    {
        return lastOffset;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        try
        {
            E element = getValue( btree );

            if ( element != null )
            {
                sb.append( btree.getName() ).append( "[" ).append( offset ).append( ", " ).append( lastOffset )
                    .append( "]:" ).append( element );
            }
            else
            {
                sb.append( btree.getName() ).append( "[" ).append( offset ).append( ", " ).append( lastOffset )
                    .append( "]" );
            }
        }
        catch ( IOException ioe )
        {
            // Nothing we can do...
        }

        return sb.toString();
    }
}
