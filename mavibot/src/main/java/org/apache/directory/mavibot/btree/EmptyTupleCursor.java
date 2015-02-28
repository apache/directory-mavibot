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
import java.util.NoSuchElementException;


/**
 * A Cursor which is used when we have no element to return
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class EmptyTupleCursor<K, V> extends TupleCursor<K, V>
{
    /** AN empty cursor does not have a revision */
    private static final long NO_REVISION = -1L;

    /** The creation date */
    private long creationDate;


    /**
     * Creates a new instance of EmptyTupleCursor. It will never return any result
     */
    public EmptyTupleCursor()
    {
        super();

        creationDate = System.currentTimeMillis();
    }


    /**
     * Change the position in the current cursor to set it after the last key
     */
    public void afterLast() throws IOException
    {
    }


    /**
     * Change the position in the current cursor before the first key
     */
    public void beforeFirst() throws IOException
    {
    }


    /**
     * Always return false.
     *
     * @return Always false
     */
    public boolean hasNext()
    {
        return false;
    }


    /**
     * Always throws a NoSuchElementException.
     *
     * @return Nothing
     * @throws NoSuchElementException There is no element in a EmptyTupleCursor
     */
    public Tuple<K, V> next() throws NoSuchElementException
    {
        throw new NoSuchElementException( "No tuple present" );
    }


    /**
     * Always throws a NoSuchElementException.
     *
     * @return Nothing
     * @throws NoSuchElementException There is no element in a EmptyTupleCursor
     */
    public Tuple<K, V> nextKey() throws NoSuchElementException
    {
        // This is the end : no more value
        throw new NoSuchElementException( "No more tuples present" );
    }


    /**
     * Always false
     *
     * @return false
     */
    public boolean hasNextKey()
    {
        return false;
    }


    /**
     * Always false
     * 
     * @return false
     */
    public boolean hasPrev()
    {
        return false;
    }


    /**
     * Always throws a NoSuchElementException.
     *
     * @return Nothing
     * @throws NoSuchElementException There is no element in a EmptyTupleCursor
     */
    public Tuple<K, V> prev() throws NoSuchElementException
    {
        throw new NoSuchElementException( "No more tuple present" );
    }


    /**
     * Always throws a NoSuchElementException.
     *
     * @return Nothing
     * @throws NoSuchElementException There is no element in a EmptyTupleCursor
     */
    public Tuple<K, V> prevKey() throws NoSuchElementException
    {
        throw new NoSuchElementException( "No more tuples present" );
    }


    /**
     * Always false
     * 
     * @return false
     */
    public boolean hasPrevKey()
    {
        return false;
    }


    /**
     * {@inheritDoc}
     */
    public void close()
    {
    }


    /**
     * Get the creation date
     * 
     * @return The creation date for this cursor
     */
    public long getCreationDate()
    {
        return creationDate;
    }


    /**
     * Always -1L for an empty cursor
     *
     * @return -1L
     */
    public long getRevision()
    {
        return NO_REVISION;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "EmptyTupleCursor";
    }
}
