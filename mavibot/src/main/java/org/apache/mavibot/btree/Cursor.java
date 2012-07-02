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

import java.util.LinkedList;

/**
 * A Cursor is used to fetch elements in a BTree and is returned by the
 * @see BTree#browse method. The cursor <strng>must</strong> be closed
 * when the user is done with it.
 * <p>
 * 
 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 */
/* No qualifier */ class Cursor<K, V>
{
    /** The transaction used for this cursor */
    private Transaction<K, V> transaction;
    
    /** The Tuple used to return the results */
    private Tuple<K, V> tuple = new Tuple<K, V>();
    
    /** The stack of pages from the root down to the leaf */
    private LinkedList<ParentPos<K, V>> stack;
    
    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     * 
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    /* No qualifier */ Cursor( Transaction<K, V> transaction, LinkedList<ParentPos<K, V>> stack  )
    {
        this.transaction = transaction;
        this.stack = stack;
    }
    
    
    /**
     * Find the next key/value
     * 
     * @return A Tuple containing the found key and value
     */
    /* No qualifier */ Tuple<K, V> next()
    {
        ParentPos<K, V> parentPos = stack.getFirst();
        
        if ( parentPos.page == null )
        {
            return new Tuple<K, V>();
        }
        
        if ( parentPos.pos == parentPos.page.getNbElems() )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findNextParentPos();
            
            if ( parentPos.page == null )
            {
                // This is the end : no more value
                return null;
            }
        }

        Leaf<K, V> leaf = (Leaf<K, V>)(parentPos.page);
        tuple.setKey( leaf.keys[parentPos.pos] );
        tuple.setValue( leaf.values[parentPos.pos] );
        
        parentPos.pos++;

        return tuple;
    }
    
    
    /**
     * Find the leaf containing the following elements.
     * 
     * @return the new ParentPos instance, or null if we have no following leaf
     */
    private ParentPos<K, V> findNextParentPos()
    {
        while ( true )
        {
            // We first go up the tree, until we reach a page which current position
            // is not the last one
            ParentPos<K, V> parentPos = stack.peek();
            
            if ( parentPos == null )
            {
                return null;
            }
            
            if ( parentPos.pos == parentPos.page.getNbElems() )
            {
                stack.pop();
                continue;
            }
            else
            {
                // Then we go down the tree until we find a leaf which position is not the last one.
                int newPos = ++parentPos.pos;
                ParentPos<K, V> newParentPos = parentPos;
                
                while ( newParentPos.page instanceof Node )
                {
                    Node<K, V> node = (Node<K, V>)newParentPos.page;
                    
                    newParentPos = new ParentPos<K, V>( node.children[newPos], 0 );
                    
                    stack.push( newParentPos );
                    
                    newPos = 0;
                }
                
                return newParentPos;
            }
        }
    }
    
    
    /**
     * Find the leaf containing the previous elements.
     * 
     * @return the new ParentPos instance, or null if we have no previous leaf
     */
    private ParentPos<K, V> findPreviousParentPos()
    {
        while ( true )
        {
            // We first go up the tree, until we reach a page which current position
            // is not the first one
            ParentPos<K, V> parentPos = stack.peek();
            
            if ( parentPos == null )
            {
                return null;
            }
            
            if ( parentPos.pos == 0 )
            {
                stack.pop();
                continue;
            }
            else
            {
                // Then we go down the tree until we find a leaf which position is not the first one.
                int newPos = --parentPos.pos;
                ParentPos<K, V> newParentPos = parentPos;
                
                while ( newParentPos.page instanceof Node )
                {
                    Node<K, V> node = (Node<K, V>)newParentPos.page;
                    
                    newParentPos = new ParentPos<K, V>( node.children[newPos], node.children[newPos].getNbElems() );
                    
                    stack.push( newParentPos );
                    
                    newPos = node.getNbElems();
                }
                
                return newParentPos;
            }
        }
    }
    
    
    /**
     * Find the previous key/value
     * 
     * @return A Tuple containing the found key and value
     */
    /* No qualifier */ Tuple<K, V> prev()
    {
        ParentPos<K, V> parentPos = stack.peek();
        
        if ( parentPos.page == null )
        {
            return new Tuple<K, V>();
        }
        
        if ( parentPos.pos == 0 )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPreviousParentPos();
            
            if ( parentPos.page == null )
            {
                // This is the end : no more value
                return null;
            }
        }

        Leaf<K, V> leaf = (Leaf<K, V>)(parentPos.page);
        
        parentPos.pos--;

        tuple.setKey( leaf.keys[parentPos.pos] );
        tuple.setValue( leaf.values[parentPos.pos] );

        return tuple;
    }
    
    
    /**
     * Tells if the cursor can return a next element
     * @return true if there are some more elements
     */
    /* No qualifier */ boolean hasNext()
    {
        ParentPos<K, V> parentPos = stack.peek();
        
        if ( parentPos.page == null )
        {
            return false;
        }
        
        if ( parentPos.pos == parentPos.page.getNbElems() )
        {
            // Remove the leaf from the stack
            stack.pop();
            
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findNextParentPos();
            
            return ( parentPos != null ) && ( parentPos.page != null );
        }
        else
        {
            return true;
        }
    }
    
    
    /**
     * Tells if the cursor can return a previous element
     * @return true if there are some more elements
     */
    /* No qualifier */ boolean hasPrev()
    {
        ParentPos<K, V> parentPos = stack.peek();
        
        if ( parentPos.page == null )
        {
            return false;
        }
        
        if ( parentPos.pos == 0 )
        {
            // Remove the leaf from the stack
            stack.pop();
            
            // Start of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPreviousParentPos();
            
            return ( parentPos != null ) && ( parentPos.page != null );
        }
        else
        {
            return true;
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
