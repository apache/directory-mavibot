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
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface Cursor<K>
{
    /** Static value for the beforeFrst and afterLast positions */
    int BEFORE_FIRST = -1;
    int AFTER_LAST = -2;


    /**
     * Tells if the cursor can return a next element
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    boolean hasNext() throws EndOfFileExceededException, IOException;


    /**
     * Tells if the cursor can return a previous element
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    boolean hasPrev() throws EndOfFileExceededException, IOException;


    /**
     * Closes the cursor, thus releases the associated transaction
     */
    void close();


    /**
     * moves the cursor to the same position that was given at the time of instantiating the cursor.
     * 
     *  For example, if the cursor was created using browse() method, then beforeFirst() will
     *  place the cursor before the 0th position.
     *  
     *  If the cursor was created using browseFrom(K), then calling beforeFirst() will reset the position
     *  to the just before the position where K is present.
     */
    void beforeFirst() throws IOException;


    /**
     * Places the cursor at the end of the last position
     * 
     * @throws IOException
     */
    void afterLast() throws IOException;
}
