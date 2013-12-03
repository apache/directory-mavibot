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
package org.apache.directory.mavibot.btree.memory;


import static org.apache.directory.mavibot.btree.memory.InternalUtil.changeNextDupsContainer;
import static org.apache.directory.mavibot.btree.memory.InternalUtil.changePrevDupsContainer;
import static org.apache.directory.mavibot.btree.memory.InternalUtil.setDupsContainer;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.TupleCursor;
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
            if ( parentPos.page == null || ( parentPos.page instanceof Node ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }

        // can happen if next() is called after prev()
        if ( parentPos.pos == BEFORE_FIRST )
        {
            parentPos.pos = 0;
        }

        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.keys[parentPos.pos] );

        if ( allowDuplicates )
        {
            MultipleMemoryHolder<K, V> mvHolder = ( MultipleMemoryHolder<K, V> ) leaf.values[parentPos.pos];

            if ( mvHolder.isSingleValue() )
            {
                tuple.setValue( mvHolder.getValue() );
                parentPos.pos++;
            }
            else
            {
                setDupsContainer( parentPos, btree );

                tuple.setValue( parentPos.valueCursor.rootPage.getKey( parentPos.dupPos ) );
                parentPos.dupPos++;

                if ( parentPos.valueCursor.getNbElems() == parentPos.dupPos )
                {
                    parentPos.pos++;
                    changeNextDupsContainer( parentPos, btree );
                }
            }
        }
        else
        {
            tuple.setValue( leaf.values[parentPos.pos].getValue() );
            parentPos.pos++;
        }

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
                child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue();
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.page = child;
                    parentPos.pos = 0;
                    child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue();
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.pos = 0;
                parentPos.page = child;
                
                if ( allowDuplicates )
                {
                    changeNextDupsContainer( parentPos, btree );
                }

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
                child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue();
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = child.getNbElems();
                    parentPos.page = child;
                    child = ((Node<K, V>)parentPos.page).children[((Node<K, V>)parentPos.page).nbElems].getValue();
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.pos = child.getNbElems();
                parentPos.page = child;

                if ( allowDuplicates )
                {
                    changePrevDupsContainer( parentPos, btree );
                }

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
            throw new NoSuchElementException( "No tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        if ( ( parentPos.pos == 0 ) && ( parentPos.dupPos == 0 ) )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPrevParentPos();

            // we also need to check for the type of page cause
            // findPrevParentPos will never return a null ParentPos
            if ( parentPos.page == null || ( parentPos.page instanceof Node ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }

        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );

        if ( allowDuplicates )
        {
            boolean posDecremented = false;

            // can happen if prev() was called after next()
            if ( parentPos.pos == parentPos.page.getNbElems() )
            {
                parentPos.pos--;
                posDecremented = true;
            }

            MultipleMemoryHolder<K, V> mvHolder = ( MultipleMemoryHolder<K, V> ) leaf.values[parentPos.pos];

            boolean prevHasSubtree = false;
            // if the current key has only one value then advance to previous position
            if ( mvHolder.isSingleValue() )
            {
                if ( !posDecremented )
                {
                    parentPos.pos--;
                    mvHolder = ( MultipleMemoryHolder<K, V> ) leaf.values[parentPos.pos];
                    posDecremented = true;
                }

                if ( mvHolder.isSingleValue() )
                {
                    tuple.setKey( leaf.keys[parentPos.pos] );
                    tuple.setValue( mvHolder.getValue() );
                }
                else
                {
                    prevHasSubtree = true;
                }
            }
            else
            {
                prevHasSubtree = true;
            }

            if ( prevHasSubtree )
            {
                setDupsContainer( parentPos, btree );

                if ( parentPos.dupPos == parentPos.valueCursor.getNbElems() )
                {
                    parentPos.dupPos--;
                }
                else if ( parentPos.dupPos == 0 )
                {
                    changePrevDupsContainer( parentPos, btree );
                    parentPos.pos--;

                    if ( parentPos.valueCursor != null )
                    {
                        parentPos.dupPos--;
                    }
                }
                else
                {
                    parentPos.dupPos--;
                }

                tuple.setKey( leaf.keys[parentPos.pos] );

                if ( parentPos.valueCursor != null )
                {
                    tuple.setValue( parentPos.valueCursor.rootPage.getKey( parentPos.dupPos ) );
                }
                else
                {
                    tuple.setValue( leaf.values[parentPos.pos].getValue() );
                }
            }
        }
        else
        {
            parentPos.pos--;
            tuple.setKey( leaf.keys[parentPos.pos] );
            tuple.setValue( leaf.values[parentPos.pos].getValue() );
        }

        return tuple;
    }


    /**
     * {@inheritDoc} 
     */
    public boolean hasNext() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return false;
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // Empty BTree, get out
            return false;
        }

        if ( parentPos.pos == AFTER_LAST )
        {
            return false;
        }
        
        if ( parentPos.pos < parentPos.page.getNbElems() )
        {
            // Not the last position, we have a next value
            return true;
        }
        else
        {
            // Check if we have some more value
            if ( allowDuplicates && ( parentPos.valueCursor != null )  && ( parentPos.dupPos < parentPos.valueCursor.getNbElems() - 1 ) )
            {
                // We have some more values
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
     * {@inheritDoc} 
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
            // Check that we are not before the first value
            if ( parentPos.pos == BEFORE_FIRST )
            {
                return false;
            }
            
            // Check if we have some more value
            if ( allowDuplicates && ( parentPos.dupPos > 0 ) )
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
     * {@inheritDoc}
     */
    public boolean hasNextKey() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            // This is the end : no more key
            return false;
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more key
            return false;
        }
        
        if ( parentPos.pos == ( parentPos.page.getNbElems() - 1 ) )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the next leaf
            parentPos = findNextParentPos();

            // we also need to check the result of the call to
            // findNextParentPos as it will return a null ParentPos
            if ( ( parentPos == null ) || ( parentPos.page == null ) )
            {
                // This is the end : no more key
                return false;
            }
            else
            {
                // We have more keys
                return true;
            }
        }
        else
        {
            return true;
        }
    }

    
    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> nextKey() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        if ( parentPos.pos == ( parentPos.page.getNbElems() - 1 ) )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the next leaf
            parentPos = findNextParentPos();

            // we also need to check the result of the call to
            // findNextParentPos as it will return a null ParentPos
            if ( ( parentPos == null ) || ( parentPos.page == null ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }
        else
        {
            // Get the next key
            parentPos.pos++;
        }

        // The key
        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.keys[parentPos.pos] );
        
        // The value
        if ( allowDuplicates )
        {
            MultipleMemoryHolder mvHolder = ( MultipleMemoryHolder<V, V> ) leaf.values[parentPos.pos];

            if( !mvHolder.isSingleValue() )
            {
                BTree<V, V> valueCursor = ( BTree ) mvHolder.getValue();
                parentPos.valueCursor = valueCursor;
                parentPos.dupPos = 0;
            }
            
            TupleCursor<V, V> cursor = parentPos.valueCursor.browse();
            cursor.beforeFirst();
            
            V value = cursor.next().getKey();
            tuple.setValue( value );
        }
        else
        {
            V value = leaf.values[parentPos.pos].getValue();
            tuple.setValue( value );
        }

        return tuple;
    }


    /**
     * {@inheritDoc}
     */
    public boolean hasPrevKey() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            // This is the end : no more key
            return false;
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more key
            return false;
        }
        
        if ( parentPos.pos == 0 )
        {
            // Beginning of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPrevParentPos();

            if ( ( parentPos == null ) || ( parentPos.page == null ) )
            {
                // This is the end : no more key
                return false;
            }
            else
            {
                return true;
            }
        }
        else
        {
            return true;
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
    public Tuple<K, V> prevKey() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        if ( parentPos.pos == 0 )
        {
            // Beginning of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPrevParentPos();

            if ( ( parentPos == null ) || ( parentPos.page == null ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }
        else
        {
            if ( parentPos.pos == AFTER_LAST )
            {
                parentPos.pos = parentPos.page.getNbElems() - 1;
            }
            else
            {
                parentPos.pos--;
            }
        }
        
        
        // Update the Tuple 
        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );

        // The key
        tuple.setKey( leaf.keys[parentPos.pos] );

        // The value
        if ( allowDuplicates )
        {
            MultipleMemoryHolder mvHolder = ( MultipleMemoryHolder<V, V> ) leaf.values[parentPos.pos];

            if( !mvHolder.isSingleValue() )
            {
                parentPos.valueCursor = ( BTree ) mvHolder.getValue();
                parentPos.dupPos = 0;
            }
            
            TupleCursor<V, V> cursor = parentPos.valueCursor.browse();
            cursor.beforeFirst();
            
            V value = cursor.next().getKey();
            tuple.setValue( value );
        }
        else
        {
            V value = leaf.values[parentPos.pos].getValue();
            tuple.setValue( value );
        }
        
        return tuple;
    }


    /**
     * {@inheritDoc}
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

            child = ((Node<K, V>)parentPos.page).children[0].getValue();
        }
        
        // and leaf
        ParentPos<K, V> parentPos = stack[depth];
        parentPos.pos = BEFORE_FIRST;

        if ( child != null )
        {
            parentPos.page = child;
        }

        if ( allowDuplicates )
        {
            setDupsContainer( parentPos, btree );
        }
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

        // Go fown the tree picking the rightmost element of each Node
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
                // We have N+1 children if the page is a Node, so we don't decrement the nbElems field
                parentPos.pos = ((Node<K, V>)parentPos.page).nbElems;
            }

            child = ((Node<K, V>)parentPos.page).children[parentPos.pos].getValue();
        }
        
        // and now, the leaf
        ParentPos<K, V> parentPos = stack[depth];

        if ( child != null )
        {
            parentPos.page = child;
        }

        parentPos.pos = AFTER_LAST;

        if ( allowDuplicates )
        {
            setDupsContainer( parentPos, btree );
        }
    }
}
