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


import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * A TupleCursor is used to fetch elements in a B-tree and is returned by the
 * @see BTree#browse method. The cursor <strong>must</strong> be closed
 * when the user is done with it.
 * <p>
 * We keep a track of the current position at each level of the B-tree, so
 * we can navigate in this B-tree forward and backward. Two special positions
 * are defined : BEFORE_FIRST and AFTER_LAST which are before the leftmost and 
 * after the rightmost elements in the B-tree
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class TupleCursor<K, V> implements Iterator<Tuple<K,V>>, Closeable
{
    /** A marker to tell that we are before the first element */
    /* No qualifier */ static final int BEFORE_FIRST = -1;

    /** A marker to tell that we are after the last element */
    /* No qualifier */ static final int AFTER_LAST = -2;

    /** The stack of pages from the root down to the leaf */
    protected ParentPos<K, V>[] stack;

    /** The stack's depth */
    protected int depth = 0;

    /** The transaction used for this cursor */
    protected Transaction transaction;


    /**
     * Creates a new instance of TupleCursor.
     */
    protected TupleCursor()
    {
    }


    /**
     * Creates a new instance of cursor, starting on a page at a given position.
     *
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     * @param depth The stack's depth
     */
    public TupleCursor( Transaction transaction, ParentPos<K, V>[] stack, int depth )
    {
        this.transaction = transaction;
        this.stack = stack;
        this.depth = depth;
    }


    /**
     * Positions this cursor after the last element. This will set the path to the
     * rightmost page for each level, and the position to the rightmost element
     * of each page.
     */
    public void afterLast() throws IOException
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }
        
        // We have to go down the stack, setting all the position to 0,
        // changing the page,except for the leaf which position is set 
        // to BEFORE_FIRST
        for ( int i = 0; i < depth; i++ )
        {
            stack[i].pos = stack[i].page.getPageNbElems();
            stack[i+1].page = ( ( Node<K, V> ) stack[i].page ).getPage( transaction, stack[i].pos );
        }

        stack[depth].pos = AFTER_LAST;
    }

    
    /**
     * Determines whether or not a call to {@link #get()} will succeed.
     *
     * @return <tt>true</tt> if a call to the {@link #get()} method will succeed, <tt>false</tt> otherwise
     */
    public boolean available()
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) || ( stack[depth].page == null ) )
        {
            return false;
        }

        // We must not be AFTER_LAST or BEFORE_FIRST 
        return ( stack[depth].pos != BEFORE_FIRST ) && ( stack[depth].pos != AFTER_LAST );
    }


    /**
     * Positions this cursor before the first element.
     */
    public void beforeFirst() throws IOException
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }

        // We have to go down the stack, setting all the position to 0,
        // changing the page,except for the leaf which position is set 
        // to BEFORE_FIRST
        for ( int i = 0; i < depth; i++ )
        {
            stack[i].pos = 0;
            stack[i+1].page = ( ( Node<K, V> ) stack[i].page ).getPage( transaction, 0 );
        }

        stack[depth].pos = BEFORE_FIRST;
    }


    /**
     * Tells if the cursor can return a next element
     *
     * @return <tt>true</tt> if there are some more elements
     */
    @Override
    public boolean hasNext()
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) || ( stack[depth].pos == AFTER_LAST ) || 
            ( stack[depth].page == null ))
        {
            return false;
        }

        // Take the leaf and check if we have no mare values
        if ( ( stack[depth].pos + 1 < stack[depth].page.getPageNbElems() ) || ( stack[depth].pos == BEFORE_FIRST ) )
        {
            // get out, we have values on the right
            return true;
        }

        // Check that we are not at position 0 for all the pages in the stack 
        for ( int i = depth - 1; i >= 0; i-- )
        {
            if ( stack[i].pos < stack[i].page.getPageNbElems() )
            {
                return true;
            }
        }
        
        // Sadly, all the pages on the stack are on pos 0 : no more prev 
        return false;
    }


    /**
     * Find the next Tuple, from the current position. If we have no more new element, 
     * a NoSuchElementException will be thrown.
     *
     * @return A Tuple containing the found key and value
     * @throws NoSuchElementException When we have reached the end of the B-tree, or if 
     * the B-tree is empty
     */
    @Override
    public Tuple<K, V> next()
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No more tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];
        
        if ( parentPos.page == null )
        {
            throw new NoSuchElementException( "No more tuple present" );
        }

        switch( parentPos.pos )
        {
            case AFTER_LAST :
                // We already are at the end of the cursor
                throw new NoSuchElementException( "No more tuples present" );

            case BEFORE_FIRST :
                // Move the the first element
                parentPos.pos = 0;
                break;
                
            default :
                // Check if we are at the end of each page, and if so, go up
                // in the stack, finding the Node that has some more children
                stack[depth].pos++;
                parentPos = stack[depth];

                if ( stack[depth].pos == stack[depth].page.getPageNbElems() )
                {
                    boolean hasNext = false;

                    // Move up to find the next leaf
                    for ( int i = depth; i >= 0; i-- )
                    {
                        if ( stack[i].pos != stack[i].page.getPageNbElems() )
                        {
                            // We have some more children on the right, move forward
                            stack[i].pos++;
    
                            // and go down the stack if we weren't already at the bottom, setting the pos to 0
                            for ( int j = i + 1; j < depth; j++ )
                            {
                                try
                                {
                                    stack[j].page = ((Node<K, V>)stack[j - 1].page).getPage( transaction, stack[j - 1].pos );
                                }
                                catch ( IOException ioe )
                                {
                                    throw new RuntimeException( ioe );
                                }
                                
                                stack[j].pos = 0;
                            }
                            
                            // The last page is a leaf
                            try
                            {
                                stack[depth].page = ((Node<K, V>)stack[depth - 1].page).getPage( transaction, stack[depth - 1].pos );
                            }
                            catch ( IOException ioe )
                            {
                                throw new RuntimeException( ioe );
                            }
                        
                            stack[depth].pos = 0;
                            
                            parentPos = stack[depth];
                            hasNext = true;
                            
                            break;
                        }
                    }
                    
                    // No next leaf : set the pos to AFTER_LAST
                    if ( !hasNext )
                    {
                        parentPos.pos = AFTER_LAST;
                    }
                }
        }

        Leaf<K, V> leaf = ( Leaf<K, V> ) parentPos.page;

        // Get the value
        V value = leaf.getValue( parentPos.pos ).get();

        // Get the key
        K key = leaf.getKey( parentPos.pos );
        
        // Build the Tuple
        return new Tuple<>( key, value );
    }


    /**
     * Tells if the cursor can return a previous element
     *
     * @return <tt>true</tt> if there are some more elements before the current one
     */
    public boolean hasPrev()
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) || ( stack[depth].pos == BEFORE_FIRST ) || ( stack[depth].page == null ))
        {
            return false;
        }

        // Take the leaf and check if we have no mare values
        if ( ( stack[depth].pos > 0 ) || ( stack[depth].pos == AFTER_LAST ) )
        {
            // get out, we have values on the left
            return true;
        }

        // Check that we are not at position 0 for all the pages in the stack 
        for ( int i = depth; i >= 0; i-- )
        {
            if ( stack[i].pos > 0 )
            {
                return true;
            }
        }
        
        // Sadly, all the pages on the stack are on pos 0 : no more prev 
        return false;
    }


    /**
     * Get the previous {@link Tuple}, from the current position. If we have no more new element, 
     * a {@link NoSuchElementException} will be thrown.
     *
     * @return A {@link Tuple} containing the found key and value
     * @throws NoSuchElementException When we have reached the beginning of the B-tree, or if 
     * the B-tree is empty
     */
    public Tuple<K, V> prev() throws IOException
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No more tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];
        
        if ( parentPos.page == null )
        {
            throw new NoSuchElementException( "No more tuple present" );
        }

        switch( parentPos.pos )
        {
            case BEFORE_FIRST :
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );

            case AFTER_LAST :
                // Move to the last element
                parentPos.pos = parentPos.page.getPageNbElems() - 1;
                break;
            
            case 0 :
                // beginning of the leaf. We have to go back into the stack up to the
                // parent, and down to the leaf on the left, if possible
                boolean hasPrev = false;
                
                for ( int i = depth - 1; i >= 0; i-- )
                {
                    if ( stack[i].pos != 0 )
                    {
                        // We have some more children on the left, move backward
                        stack[i].pos--;

                        // and go down the stack if we weren't already at the bottom, setting the pos to nbElems
                        for ( int j = i + 1; j < depth; j++ )
                        {
                            stack[j].page = ((Node<K, V>)stack[j - 1].page).getPage( transaction, stack[j - 1].pos );
                            stack[j].pos = stack[j].page.getPageNbElems();
                        }

                        // The last page is a leaf
                        stack[depth].page = ((Node<K, V>)stack[depth - 1].page).getPage( transaction, stack[depth - 1].pos );
                        stack[depth].pos = stack[depth].page.getPageNbElems() - 1;
                        
                        hasPrev = true;
                        break;
                    }
                }

                // we also need to check for the type of page cause
                // findPrevParentPos will never return a null ParentPos
                if ( !hasPrev )
                {
                    // This is the end : no more value
                    throw new NoSuchElementException( "No more tuples present" );
                }
                
                break;
                
            default :
                // Simply decrement the position
                parentPos.pos--;
                break;
        }

        // Get the value
        ValueHolder<V> valueHolder = ( ( Leaf<K, V> ) parentPos.page ).getValue( parentPos.pos );
        V value = valueHolder.get();

        // Get the key
        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
        K key = leaf.getKey( parentPos.pos );
        
        // Construct the Tuple 
        return new Tuple<>( key, value );
    }


    /**
     * Checks if this cursor is closed. Calls to this operation should not
     * fail with exceptions if and only if the cursor is in the closed state.
     *
     * @return <tt>true</tt> if this cursor is closed, <tt>false</tt> otherwise
     */
    public boolean isClosed()
    {
        return transaction.isClosed();
    }


    /**
     * Closes this cursor and frees any resources it my have allocated.
     * Repeated calls to this method after this cursor has already been
     * called should not fail with exceptions.
     */
    @Override
    public void close()
    {
        //try 
        {
            //transaction.close();
        }
        //catch ( IOException ioe )
        {
            // There is nothing we can do
        }
    }


    /**
     * Closes this cursor and frees any resources it my have allocated.
     * Repeated calls to this method after this cursor has already been
     * called should not fail with exceptions.  The reason argument is 
     * the Exception instance thrown instead of the standard 
     * CursorClosedException.
     *
     * @param reason exception thrown when this cursor is accessed after close
     */
    public void close( Exception reason )
    {
    }


    /**
     * Gets the object at the current position.  cursor implementations may
     * choose to reuse element objects by re-populating them on advances
     * instead of creating new objects on each advance.
     *
     * @return the object at the current position
     * @throws NoSuchElementException if the object at this cursor's current position
     * cannot be retrieved, or if this cursor is closed
     */
    public Tuple<K, V> get()
    {
        // First check that we have elements in the B-tree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];
        
        // Check that we actually have elements
        if ( parentPos.page == null )
        {
            // Empty B-tree, get out
            throw new NoSuchElementException( "No tuple present" );
        }
        
        if ( ( parentPos.pos != BEFORE_FIRST ) && ( parentPos.pos != AFTER_LAST ) )
        {
            // We have some element, build the Tuple
            Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );

            // Get the value
            V value = leaf.getValue( parentPos.pos ).get();

            // Get the key
            K key = leaf.getKey( parentPos.pos );
            
            // Build the Tuple
            return new Tuple<>( key, value );
        }
        
        throw new NoSuchElementException( "No element at this position : " + parentPos.pos );
    }


    /**
     * Get the creation date
     * @return The creation date for this cursor
     */
    public long getCreationDate()
    {
        return ((ReadTransaction)transaction).getCreationDate();
    }


    /**
     * Get the current revision
     *
     * @return The revision this cursor is based on
     */
    public long getRevision()
    {
        return transaction.getRevision();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "TupleCursor, depth = " ).append( depth ).append( "\n" );

        for ( int i = 0; i <= depth; i++ )
        {
            sb.append( "    " ).append( stack[i] ).append( "\n" );
        }

        return sb.toString();
    }
}
