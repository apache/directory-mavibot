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

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A Cursor which is used when we have no element to return
 * <p>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class EmptyTupleCursor<K, V> extends TupleCursor<K, V>
{
    private long revision;
    private long creationDate;

    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     *
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    public EmptyTupleCursor( long revision )
    {
        super();

        this.revision = revision;
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
     * Tells if the cursor can return a next element
     *
     * @return true if there are some more elements
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public boolean hasNext() throws EndOfFileExceededException, IOException
    {
        return false;
    }


    /**
     * Find the next key/value
     *
     * @return A Tuple containing the found key and value
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public Tuple<K, V> next() throws EndOfFileExceededException, IOException
    {
        throw new NoSuchElementException( "No tuple present" );
    }


    /**
     * Get the next non-duplicate key.
     * If the BTree contains :
     *
     *  <ul>
     *    <li><1,0></li>
     *    <li><1,1></li>
     *    <li><1,2></li>
     *    <li><2,0></li>
     *    <li><2,1></li>
     *  </ul>
     *
     *  and cursor is present at <1,1> then the returned tuple will be <2,0> (not <1,2>)
     *
     * @return A Tuple containing the found key and value
     * @throws EndOfFileExceededException
     * @throws IOException
     */
    public Tuple<K, V> nextKey() throws EndOfFileExceededException, IOException
    {
        // This is the end : no more value
        throw new NoSuchElementException( "No more tuples present" );
    }


    /**
     * Tells if the cursor can return a next key
     *
     * @return true if there are some more keys
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public boolean hasNextKey() throws EndOfFileExceededException, IOException
    {
        return false;
    }


    /**
     * Tells if the cursor can return a previous element
     *
     * @return true if there are some more elements
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public boolean hasPrev() throws EndOfFileExceededException, IOException
    {
        return false;
    }


    /**
     * Find the previous key/value
     *
     * @return A Tuple containing the found key and value
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public Tuple<K, V> prev() throws EndOfFileExceededException, IOException
    {
        throw new NoSuchElementException( "No more tuple present" );
    }


    /**
     * Get the previous non-duplicate key.
     * If the BTree contains :
     *
     *  <ul>
     *    <li><1,0></li>
     *    <li><1,1></li>
     *    <li><1,2></li>
     *    <li><2,0></li>
     *    <li><2,1></li>
     *  </ul>
     *
     *  and cursor is present at <2,1> then the returned tuple will be <1,0> (not <2,0>)
     *
     * @return A Tuple containing the found key and value
     * @throws EndOfFileExceededException
     * @throws IOException
     */
    public Tuple<K, V> prevKey() throws EndOfFileExceededException, IOException
    {
        throw new NoSuchElementException( "No more tuples present" );
    }


    /**
     * Tells if the cursor can return a previous key
     *
     * @return true if there are some more keys
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public boolean hasPrevKey() throws EndOfFileExceededException, IOException
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
     * @return The creation date for this cursor
     */
    public long getCreationDate()
    {
        return creationDate;
    }


    /**
     * Get the current revision
     *
     * @return The revision this cursor is based on
     */
    public long getRevision()
    {
        return revision;
    }


    public String toString()
    {
        return "EmptyTupleCursor";
    }
}
