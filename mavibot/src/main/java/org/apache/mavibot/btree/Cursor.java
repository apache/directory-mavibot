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

/**
 * A Cursor is used to fetch elements in a BTree and is returned by the
 * @see BTree#browse method. The cursor <strng>must</strong> be closed
 * when the user is done with it.
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 */
public class Cursor<K, V>
{
    /** The transaction used for this cursor */
    private Transaction<K, V> transaction;
    
    /** The current leaf */
    private Leaf<K, V> leaf;
    
    /** The current position in the leaf */
    private int pos;
    
    /** The Tuple used to return the results */
    private Tuple<K, V> tuple = new Tuple<K, V>();
    
    
    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     * 
     * @param transaction The transaction this operation is protected by
     * @param leaf The page in which the first element is present
     * @param pos The position of the first element
     */
    public Cursor( Transaction<K, V> transaction, Leaf<K, V> leaf, int pos )
    {
        this.transaction = transaction;
        this.leaf = leaf;
        this.pos = pos;
    }
    
    
    /**
     * Find the next key/value
     * 
     * @return A Tuple containing the found key and value
     */
    public Tuple<K, V> next()
    {
        if ( leaf == null )
        {
            return new Tuple<K, V>();
        }
        
        if ( pos == leaf.nbElems )
        {
            if ( leaf.nextPage == null )
            {
                // This is the end : no more value
                return null;
            }
            else
            {
                leaf = leaf.nextPage;
                pos = 0;
            }
        }

        tuple.setKey( leaf.keys[pos] );
        tuple.setValue( leaf.values[pos] );
        
        pos++;

        return tuple;
    }
    
    
    /**
     * Find the previous key/value
     * 
     * @return A Tuple containing the found key and value
     */
    public Tuple<K, V> prev()
    {
        if ( pos == 0 )
        {
            if ( leaf.prevPage == null )
            {
                // This is the end : no more value
                return null;
            }
            else
            {
                leaf = leaf.prevPage;
                pos = leaf.getNbElems();
            }
        }

        pos--;

        tuple.setKey( leaf.keys[pos] );
        tuple.setValue( leaf.values[pos] );

        return tuple;
    }
    
    
    /**
     * Tells if the cursor can return a next element
     * @return true if there are some more elements
     */
    public boolean hasNext()
    {
        if ( leaf == null )
        {
            return false;
        }
        
        if ( pos < leaf.nbElems )
        {
            return true;
        }
        else
        {
            if ( leaf.nextPage == null )
            {
                // This is the end : no more value
                return false;
            }
            else
            {
                return true;
            }
        }
    }
    
    
    /**
     * Tells if the cursor can return a previous element
     * @return true if there are some more elements
     */
    public boolean hasPrev()
    {
        if ( leaf == null )
        {
            return false;
        }
        
        if ( pos > 0 )
        {
            return true;
        }
        else
        {
            if ( leaf.prevPage == null )
            {
                // This is the end : no more value
                return false;
            }
            else
            {
                return true;
            }
        }
    }
    
    
    /**
     * Closes the cursor, thus releases the associated transaction
     */
    public void close()
    {
        transaction.close();
    }
}
