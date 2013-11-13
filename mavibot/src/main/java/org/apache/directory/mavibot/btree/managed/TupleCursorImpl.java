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
package org.apache.directory.mavibot.btree.managed;


import static org.apache.directory.mavibot.btree.managed.InternalUtil.changeNextDupsContainer;
import static org.apache.directory.mavibot.btree.managed.InternalUtil.changePrevDupsContainer;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.TupleCursor;
import org.apache.directory.mavibot.btree.ValueCursor;
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
public class TupleCursorImpl<K, V> implements TupleCursor<K, V>
{
    /** The transaction used for this cursor */
    private Transaction<K, V> transaction;

    /** The Tuple used to return the results */
    private Tuple<K, V> tuple = new Tuple<K, V>();

    /** The stack of pages from the root down to the leaf */
    private ParentPos<K, V>[] stack;
    
    /** The stack's depth */
    private int depth = 0;

    /** The BTree we are walking */
    private BTree<K, V> btree;

    private boolean allowDuplicates;


    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     * 
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    TupleCursorImpl( BTree<K, V> btree, Transaction<K, V> transaction, ParentPos<K, V>[] stack, int depth )
    {
        this.transaction = transaction;
        this.stack = stack;
        this.btree = btree;
        this.allowDuplicates = btree.isAllowDuplicates();
        this.depth = depth;
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
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        if ( parentPos.pos == parentPos.page.getNbElems() )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findNextParentPos();

            // we also need to check for the type of page cause
            // findNextParentPos will never return a null ParentPos
            if ( ( parentPos == null ) || ( parentPos.page == null ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }

        V value = null;
        
        if ( parentPos.valueCursor.hasNext() )
        {
            value = parentPos.valueCursor.next();
        }
        else
        {
            if ( parentPos.pos == parentPos.page.getNbElems() - 1 )
            {
                parentPos = findNextParentPos();

                if ( ( parentPos == null ) || ( parentPos.page == null ) )
                {
                    // This is the end : no more value
                    throw new NoSuchElementException( "No more tuples present" );
                }
            }
            else
            {
                parentPos.pos++;
            }
                
            try
            {
                ValueHolder<V> valueHolder = ( ( Leaf<K, V> ) parentPos.page ).getValue( parentPos.pos );
                
                parentPos.valueCursor = valueHolder.getCursor();
                
                value = parentPos.valueCursor.next();
            }
            catch ( IllegalArgumentException e )
            {
                e.printStackTrace();
            }
        }
        
        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.keys[parentPos.pos].getKey() );
        tuple.setValue( value );

        return tuple;
    }


    /**
     * Find the leaf containing the following elements.
     * 
     * @return the new ParentPos instance, or null if we have no following leaf
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private ParentPos<K, V> findNextParentPos() throws EndOfFileExceededException, IOException
    {
        if ( depth == 0 )
        {
            // No need to go any further, there is only one leaf in the btree
            return null;
        }

        int currentDepth = depth - 1;
        Page<K, V> child = null;

        // First, go up the tree until we find a Node which has some element on the right
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, V> parentPos = stack[currentDepth];

            if ( parentPos.pos == parentPos.page.getNbElems() )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                parentPos.pos++;
                child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue( btree );
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.page = child;
                    parentPos.pos = 0;
                    child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue( btree );
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.pos = 0;
                parentPos.page = child;
                parentPos.valueCursor = ((Leaf<K, V>)child).values[0].getCursor();

                return parentPos;
            }
        }
        
        return null;
    }
    
    
    /**
     * Find the leaf containing the previous elements.
     * 
     * @return the new ParentPos instance, or null if we have no previous leaf
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private ParentPos<K, V> findPrevParentPos() throws EndOfFileExceededException, IOException
    {
        if ( depth == 0 )
        {
            // No need to go any further, there is only one leaf in the btree
            return null;
        }

        int currentDepth = depth - 1;
        Page<K, V> child = null;

        // First, go up the tree until we find a Node which has some element on the left
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, V> parentPos = stack[currentDepth];

            if ( parentPos.pos == 0 )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                parentPos.pos--;
                child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue( btree );
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = child.getNbElems();
                    parentPos.page = child;
                    child = ((Node<K, V>)parentPos.page).children[((Node<K, V>)parentPos.page).nbElems].getValue( btree );
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.pos = child.getNbElems() - 1;
                parentPos.page = child;
                ValueHolder<V> valueHolder = ((Leaf<K, V>)parentPos.page).values[parentPos.pos];
                parentPos.valueCursor = valueHolder.getCursor();
                parentPos.valueCursor.afterLast();

                return parentPos;
            }
        }
        
        return null;
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
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No more tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        if ( ( parentPos.pos == 0 ) && ( !parentPos.valueCursor.hasPrev() ) )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPrevParentPos();

            // we also need to check for the type of page cause
            // findPrevParentPos will never return a null ParentPos
            if ( ( parentPos == null ) || ( parentPos.page == null ) || ( parentPos.page instanceof Node ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }
        
        V value = null;
        
        if ( parentPos.valueCursor.hasPrev() )
        {
            value = parentPos.valueCursor.prev();
        }
        else
        {
            if ( parentPos.pos == 0 )
            {
                parentPos = findPrevParentPos();

                if ( ( parentPos == null ) || ( parentPos.page == null ) )
                {
                    // This is the end : no more value
                    throw new NoSuchElementException( "No more tuples present" );
                }
            }
            else
            {
                parentPos.pos--;
                
                try
                {
                    ValueHolder<V> valueHolder = ( ( Leaf<K, V> ) parentPos.page ).getValue( parentPos.pos );
                    
                    parentPos.valueCursor = valueHolder.getCursor();
                    parentPos.valueCursor.afterLast();
                    
                    value = parentPos.valueCursor.prev();
                }
                catch ( IllegalArgumentException e )
                {
                    e.printStackTrace();
                }
            }
        }


        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.keys[parentPos.pos].getKey() );
        tuple.setValue( value );

        return tuple;
    }


    /**
     * Tells if the cursor can return a next tupe.
     * 
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasNext() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return false;
        }
        
        // Take the leaf and check if we have no mare values
        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // Empty BTree, get out
            return false;
        }

        if ( parentPos.pos < parentPos.page.getNbElems() - 1 )
        {
            // Not the last position, we have a next value
            return true;
        }
        else
        {
            // Check if we have some more value
            if ( parentPos.valueCursor.hasNext() )
            {
                return true;
            }
            
            // Ok, here, we have reached the last value in the leaf. We have to go up and 
            // see if we have some remaining values
            int currentDepth = depth - 1;
            
            while ( currentDepth >= 0 )
            {
                parentPos = stack[currentDepth];
                
                if ( parentPos.pos < parentPos.page.getNbElems() )
                {
                    // The parent has some remaining values on the right, get out
                    return true;
                }
                else
                {
                    currentDepth --;
                }
            }
            
            // We are done, there are no more value left
            return false;
        }
    }


    /**
     * Tells if the cursor can return a previous element
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasPrev() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return false;
        }

        // Take the leaf and check if we have no mare values
        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // Empty BTree, get out
            return false;
        }

        if ( parentPos.pos > 0 )
        {
            // get out, we have values on the left
            return true;
        }
        else
        {
            // Check if we have some more value
            if ( parentPos.valueCursor.hasPrev() )
            {
                return true;
            }

            // Ok, here, we have reached the first value in the leaf. We have to go up and 
            // see if we have some remaining values
            int currentDepth = depth - 1;
            
            while ( currentDepth >= 0 )
            {
                parentPos = stack[currentDepth];
                
                if ( parentPos.pos > 0 )
                {
                    // The parent has some remaining values on the right, get out
                    return true;
                }
                else
                {
                    currentDepth --;
                }
            }
            
            return false;
        }
    }


    /**
     * Closes the cursor, thus releases the associated transaction
     */
    public void close()
    {
        transaction.close();
    }


    /**
     * @return The revision this cursor is based on
     */
    public long getRevision()
    {
        return transaction.getRevision();
    }


    /**
     * @return The creation date for this cursor
     */
    public long getCreationDate()
    {
        return transaction.getCreationDate();
    }


    /**
     * Moves the cursor to the next non-duplicate key.

     * If the BTree contains 
     * 
     *  <ul>
     *    <li><1,0></li>
     *    <li><1,1></li>
     *    <li><2,0></li>
     *    <li><2,1></li>
     *  </ul>
     *   
     *  and cursor is present at <1,0> then the cursor will move to <2,0>
     *  
     * @throws EndOfFileExceededException
     * @throws IOException
     */
    public void moveToNextNonDuplicateKey() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            return;
        }

        if ( parentPos.pos == ( parentPos.page.getNbElems() - 1 ) )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            // increment the position cause findNextParentPos checks "parentPos.pos == parentPos.page.getNbElems()"
            parentPos.pos++;
            ParentPos<K, V> nextPos = findNextParentPos();

            // if the returned value is a Node OR if it is same as the parentPos
            // that means cursor is already at the last position
            // call afterLast() to restore the stack with the path to the right most element
            if ( ( nextPos.page instanceof Node ) || ( nextPos == parentPos ) )
            {
                afterLast();
            }
            else
            {
                parentPos = nextPos;
            }
        }
        else
        {
            parentPos.pos++;
            changeNextDupsContainer( parentPos, btree );
        }
    }


    /**
     * Moves the cursor to the previous non-duplicate key
     * If the BTree contains 
     * 
     *  <ul>
     *    <li><1,0></li>
     *    <li><1,1></li>
     *    <li><2,0></li>
     *    <li><2,1></li>
     *  </ul>
     *   
     *  and cursor is present at <2,1> then the cursor will move to <1,1>
     * 
     * @throws EndOfFileExceededException
     * @throws IOException
     */
    public void moveToPrevNonDuplicateKey() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            return;
        }

        if ( parentPos.pos == 0 )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPrevParentPos();

            // if the returned value is a Node that means cursor is already at the first position
            // call beforeFirst() to restore the stack to the initial state
            if ( parentPos.page instanceof Node )
            {
                beforeFirst();
            }
        }
        else
        {
            changePrevDupsContainer( parentPos, btree );
            parentPos.pos--;
        }
    }


    /**
     * moves the cursor to the same position that was given at the time of instantiating the cursor.
     * 
     *  For example, if the cursor was created using browse() method, then beforeFirst() will
     *  place the cursor before the 0th position.
     *  
     *  If the cursor was created using browseFrom(K), then calling beforeFirst() will reset the position
     *  to the just before the position where K is present.
     */
    public void beforeFirst() throws IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }

        Page<K, V> child = null;
        
        for ( int i = 0; i < depth; i++ )
        {
            ParentPos<K, V> parentPos = stack[i];
            parentPos.pos = 0;
            
            if ( child != null )
            {
                parentPos.page = child;
            }
            
            child = ((Node<K, V>)parentPos.page).children[0].getValue( btree );
        }
        
        // and leaf
        ParentPos<K, V> parentPos = stack[depth];
        parentPos.pos = 0;
        
        if ( child == null )
        {
            child = parentPos.page;
        }
        else
        {
            parentPos.page = child;
        }
        
        parentPos.valueCursor = ((Leaf<K, V>)parentPos.page).values[parentPos.pos].getCursor();
        parentPos.valueCursor.beforeFirst();
    }


    /**
     * Places the cursor at the end of the last position
     * 
     * @throws IOException
     */
    public void afterLast() throws IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }

        Page<K, V> child = null;

        for ( int i = 0; i < depth; i++ )
        {
            ParentPos<K, V> parentPos = stack[i];
            
            if ( child != null )
            {
                parentPos.page = child;
                parentPos.pos = ((Node<K, V>)child).nbElems;
            }
            else
            {
                parentPos.pos = ((Node<K, V>)parentPos.page).nbElems;
            }

            child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue( btree );
        }
        
        // and leaf
        ParentPos<K, V> parentPos = stack[depth];

        if ( child == null )
        {
            child = parentPos.page;
            parentPos.pos = ((Leaf<K, V>)parentPos.page).nbElems - 1;
        }
        else
        {
            parentPos.page = child;
            parentPos.pos = ((Leaf<K, V>)child).nbElems - 1;
        }

        parentPos.valueCursor = ((Leaf<K, V>)parentPos.page).values[parentPos.pos].getCursor();
        parentPos.valueCursor.afterLast();
    }
}
