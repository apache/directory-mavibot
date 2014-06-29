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
import org.apache.directory.mavibot.btree.exception.KeyNotFoundException;
import org.apache.directory.mavibot.btree.BTree;


/**
 * A class that encapsulate the values into an sub-btree
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */class ValueBTreeCursor<V> implements ValueCursor<V>
{
    /** Store the current position in the array or in the BTree */
    private KeyCursor<V> cursor;

    /** The Value sub-btree */
    private BTree<V, V> valueBtree;


    /**
     * Create an instance
     */
    public ValueBTreeCursor( BTree<V, V> valueBtree )
    {
        this.valueBtree = valueBtree;

        // Start at -1 to be positioned before the first element
        try
        {
            if ( valueBtree != null )
            {
                cursor = valueBtree.browseKeys();
            }
        }
        catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch ( KeyNotFoundException knfe )
        {
            // TODO Auto-generated catch block
            knfe.printStackTrace();
        }
    }


    /**
     * {@inheritDoc}}
     */
    @Override
    public boolean hasNext()
    {
        if ( cursor == null )
        {
            return false;
        }
        else
        {
            try
            {
                return cursor.hasNext();
            }
            catch ( EndOfFileExceededException e )
            {
                e.printStackTrace();
                return false;
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                return false;
            }
        }
    }


    /**
     * {@inheritDoc}}
     */
    public V next()
    {
        try
        {
            return cursor.next();
        }
        catch ( EndOfFileExceededException e )
        {
            e.printStackTrace();
            return null;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * {@inheritDoc}}
     */
    @Override
    public boolean hasPrev() throws EndOfFileExceededException, IOException
    {
        if ( cursor == null )
        {
            return false;
        }
        else
        {
            try
            {
                return cursor.hasPrev();
            }
            catch ( EndOfFileExceededException e )
            {
                e.printStackTrace();
                return false;
            }
            catch ( IOException e )
            {
                e.printStackTrace();
                return false;
            }
        }
    }


    /**
     * {@inheritDoc}}
     */
    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
        }
    }


    /**
     * {@inheritDoc}}
     */
    @Override
    public void beforeFirst() throws IOException
    {
        if ( cursor != null )
        {
            cursor.beforeFirst();
        }
    }


    /**
     * {@inheritDoc}}
     */
    @Override
    public void afterLast() throws IOException
    {
        if ( cursor != null )
        {
            cursor.afterLast();
        }
    }


    /**
     * {@inheritDoc}}
     */
    @Override
    public V prev() throws EndOfFileExceededException, IOException
    {
        try
        {
            return cursor.prev();
        }
        catch ( EndOfFileExceededException e )
        {
            e.printStackTrace();
            return null;
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public int size()
    {
        return ( int ) valueBtree.getNbElems();
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "BTreeCursor";
    }
}
