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

import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A Cursor is used to fetch elements in a BTree and is returned by the
 * @see BTree#browse method. The cursor <strng>must</strong> be closed
 * when the user is done with it.
 * <p>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface TupleCursor<K, V> extends Cursor<K>
{
    /**
     * Tells if the cursor can return a next element
     * 
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasNext() throws EndOfFileExceededException, IOException;

    /**
     * Find the next key/value
     * 
     * @return A Tuple containing the found key and value
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    Tuple<K, V> next() throws EndOfFileExceededException, IOException;


    /**
     * Tells if the cursor can return a previous element
     * 
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasPrev() throws EndOfFileExceededException, IOException;
    
    
    /**
     * Find the previous key/value
     * 
     * @return A Tuple containing the found key and value
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    Tuple<K, V> prev() throws EndOfFileExceededException, IOException;


    /**
     * @return The revision this cursor is based on
     */
    long getRevision();


    /**
     * @return The creation date for this cursor
     */
    long getCreationDate();


    /**
     * Tells if the cursor can return a next key
     * 
     * @return true if there are some more keys
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasNextKey() throws EndOfFileExceededException, IOException;
    
    
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
    Tuple<K, V> nextKey() throws EndOfFileExceededException, IOException;


    /**
     * Tells if the cursor can return a previous key
     * 
     * @return true if there are some more keys
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasPrevKey() throws EndOfFileExceededException, IOException;

    
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
    Tuple<K, V> prevKey() throws EndOfFileExceededException, IOException;
    
    
    /**
     * Change the position in the current cursor tbefore the first key
     */
    void beforeFirst() throws IOException;
    
    
    /**
     * Change the position in the current cursor to set it after the last key
     */
    void afterLast() throws IOException;
}
