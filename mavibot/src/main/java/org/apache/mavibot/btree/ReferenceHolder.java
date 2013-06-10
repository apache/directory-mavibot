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
package org.apache.mavibot.btree;


import java.io.IOException;
import java.lang.ref.SoftReference;

import org.apache.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A Value holder. As we may not store all the values in memory (except for an in-memory
 * BTree), we will use a SoftReference to keep a reference to a Value, and if it's null,
 * then we will load the Value from the underlying physical support, using the offset. 
 * 
 * @param <E> The type for the stored element (either a value or a page)
 * @param <K> The type of the BTree key
 * @param <V> The type of the BTree value
 *
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class ReferenceHolder<E, K, V> implements ElementHolder<E, K, V>
{
    /** The BTree */
    private BTree<K, V> btree;

    /** The offset for a value stored on disk */
    private long offset;

    /** The reference to the element instance, or null if it's not present */
    private SoftReference<E> reference;


    /**
     * Create a new holder storing an offset and a SoftReference containing the element.
     * 
     * @param offset The offset in disk for this value
     * @param element The element to store into a SoftReference
     */
    public ReferenceHolder( BTree<K, V> btree, E element, long offset )
    {
        this.btree = btree;
        this.offset = offset;
        this.reference = new SoftReference<E>( element );
    }


    /**
     * {@inheritDoc}
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    @Override
    public E getValue( BTree<K, V> btree ) throws EndOfFileExceededException, IOException
    {
        E element = reference.get();

        if ( element == null )
        {
            // We have to fetch the element from disk, using the offset now
            element = fetchElement( btree );
            reference = new SoftReference( element );
        }

        return element;
    }


    /**
     * Retrieve the value from the disk, using the BTree and offset
     * @return The deserialized element (
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private E fetchElement( BTree<K, V> btree ) throws EndOfFileExceededException, IOException
    {
        E element = ( E ) btree.getRecordManager().deserialize( btree, offset );

        return element;
    }


    /* No qualifier */long getOffset()
    {
        return offset;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        E element = reference.get();

        if ( element != null )
        {
            sb.append( btree.getName() ).append( "[" ).append( offset ).append( "]:" ).append( element );
        }
        else
        {
            sb.append( btree.getName() ).append( "[" ).append( offset ).append( "]" );
        }

        return sb.toString();
    }
}
