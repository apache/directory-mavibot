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
 * A class that encapsulate the values into an array
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */class ValueArrayCursor<V> implements ValueCursor<V>
{
    /** Store the current position in the array or in the BTree */
    private int currentPos;

    /** The array storing values (1 to N) */
    private V[] valueArray;


    /**
     * Create an instance
     */
    public ValueArrayCursor( V[] valueArray )
    {
        // Start at -1 to be positioned before the first element
        currentPos = BEFORE_FIRST;
        this.valueArray = valueArray;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext()
    {
        return ( currentPos < valueArray.length - 1 ) && ( currentPos != AFTER_LAST );
    }


    /**
     * {@inheritDoc}
     */
    public V next()
    {
        if ( valueArray == null )
        {
            // Deserialize the array
            return null;
        }
        else
        {
            currentPos++;

            if ( currentPos == valueArray.length )
            {
                currentPos = AFTER_LAST;

                // We have reached the end of the array
                return null;
            }
            else
            {
                return valueArray[currentPos];
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasPrev() throws EndOfFileExceededException, IOException
    {
        return currentPos > 0 || currentPos == AFTER_LAST;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void beforeFirst() throws IOException
    {
        currentPos = BEFORE_FIRST;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void afterLast() throws IOException
    {
        currentPos = AFTER_LAST;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public V prev() throws EndOfFileExceededException, IOException
    {
        if ( valueArray == null )
        {
            // Deserialize the array
            return null;
        }
        else
        {
            if ( currentPos == AFTER_LAST )
            {
                currentPos = valueArray.length - 1;
            }
            else
            {
                currentPos--;
            }

            if ( currentPos == BEFORE_FIRST )
            {
                // We have reached the end of the array
                return null;
            }
            else
            {
                return valueArray[currentPos];
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        return valueArray.length;
    }
}
