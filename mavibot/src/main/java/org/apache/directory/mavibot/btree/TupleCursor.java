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

import org.apache.directory.mavibot.btree.exception.CursorException;
import org.apache.directory.mavibot.btree.exception.EndOfFileExceededException;


/**
 * A Cursor is used to fetch elements in a BTree and is returned by the
 * @see BTree#browse method. The cursor <strong>must</strong> be closed
 * when the user is done with it.
 * <p>
 * We keep a track of the current position at each level of the B-tree, so
 * we can navigate in this B-tree forward and backward. Two special positions
 * are defined : BEFORE_FIRST and AFTER_LAST which are befoe the leftmost and 
 * after the rightmost elements in the B-tree
 *
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class TupleCursor<K, V> //implements Iterator<Tuple<K,V>>
{
    /** A marker to tell that we are before the first element */
    private static final int BEFORE_FIRST = -1;

    /** A marker to tell that we are after the last element */
    private static final int AFTER_LAST = -2;

    /** The stack of pages from the root down to the leaf */
    protected ParentPos<K, V>[] stack;

    /** The stack's depth */
    protected int depth = 0;

    /** The transaction used for this cursor */
    protected ReadTransaction<K, V> transaction;


    /**
     * Creates a new instance of Cursor.
     */
    protected TupleCursor()
    {
    }


    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     *
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    public TupleCursor( ReadTransaction<K, V> transaction, ParentPos<K, V>[] stack, int depth )
    {
        this.transaction = transaction;
        this.stack = stack;
        this.depth = depth;
    }


    /**
     * Positions this Cursor after the last element. This will set the path to the
     * rightmost page for each level, and the position to the rightmost element
     * of each page.
     *
     * @throws CursorException if there are problems positioning this Cursor or if
     * this Cursor is closed
     */
    public void afterLast() throws CursorException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }
        
        // Update the parent page.
        ParentPos<K, V> parentPos = stack[0];
        parentPos.pos = parentPos.page.getNbElems();
        Page<K, V> childPage = null;

        if ( parentPos.page instanceof Node )
        {
            childPage = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos );
        }

        // Go down the Tree, selecting the rightmost element in each Node
        for ( int i = 1; i < depth; i++ )
        {
            parentPos = stack[i];

            // We are in the middle of the tree, change the parentPos to be
            // the one we have selected previously
            // Set the parentPos to be at the rightmost position for this level.
            parentPos.page = childPage;
            parentPos.pos = childPage.getNbElems();

            // Get the child page, and iterate
            childPage = ( ( AbstractPage<K, V> ) childPage ).getPage( parentPos.pos );
        }

        // Now, we should handle the leaf
        parentPos = stack[depth];

        if ( childPage != null )
        {
            // The tree had more than one level, so we make the current parentPos
            // point on the previously selected leaf.
            parentPos.page = childPage;
        }

        // Finally, set the position after the rightmost element
        parentPos.pos = AFTER_LAST;
    }

    
    /**
     * Determines whether or not a call to get() will succeed.
     *
     * @return true if a call to the get() method will succeed, false otherwise
     */
    public boolean available()
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
        
        // We must not be AFTER_LAST or BEFORE_FIRST 
        return ( parentPos.pos != BEFORE_FIRST ) && ( parentPos.pos != AFTER_LAST );
    }


    /**
     * Positions this Cursor before the first element.
     *
     * @throws Exception if there are problems positioning this cursor or if
     * this Cursor is closed
     */
    public void beforeFirst() throws CursorException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            return;
        }

        ParentPos<K, V> parentPos = stack[0];
        parentPos.pos = 0;
        Page<K, V> child = null;
        
        if ( parentPos.page instanceof Node )
        {
            child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( 0 );
        }

        for ( int i = 1; i < depth; i++ )
        {
            parentPos = stack[i];
            parentPos.pos = 0;
            parentPos.page = child;

            child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( 0 );
        }

        // and leaf
        parentPos = stack[depth];
        parentPos.pos = BEFORE_FIRST;

        if ( child != null )
        {
            parentPos.page = child;
        }
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

        if ( parentPos.pos == AFTER_LAST )
        {
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
                    currentDepth--;
                }
            }

            return false;
        }

        if ( parentPos.pos < parentPos.page.getNbElems() - 1 )
        {
            // Not the last position, we have a next value
            return true;
        }
        else
        {
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
                    currentDepth--;
                }
            }

            // We are done, there are no more value left
            return false;
        }
    }


    /**
     * Find the next Tuple, from the current position. If we have no more new element, 
     * a NoSuchElementException will be thrown.
     *
     * @return A Tuple containing the found key and value
     * @throws IOException
     * @throws NoSuchElementException When we have reached the end of the B-tree, or if 
     * the B-tree is empty
     */
    public Tuple<K, V> next() throws NoSuchElementException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        try
        {
            if ( parentPos.pos == parentPos.page.getNbElems() )
            {
                // End of the leaf. We have to go back into the stack up to the
                // parent, and down to the leaf
                parentPos = findNextParentPos();
    
                // we also need to check for the type of page cause
                // findNextParentPos will never return a null ParentPos
                if ( parentPos == null )
                {
                    // This is the end : no more value
                    throw new NoSuchElementException( "No more tuples present" );
                }
            }
            else
            {
                parentPos.pos++;
            }
        }
        catch ( IOException ioe )
        {
            throw new NoSuchElementException( ioe.getMessage());
        }

        // Get the value
        ValueHolder<V> valueHolder = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos );

        V value = valueHolder.get();

        // Get the key
        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
        K key = leaf.getKey( parentPos.pos );
        
        // Build the Tuple
        Tuple<K, V> tuple = new Tuple<K, V>( key, value );

        return tuple;
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
                    currentDepth--;
                }
            }

            return false;
        }
    }


    /**
     * Get the previous Tuple, from the current position. If we have no more new element, 
     * a NoMoreElementException will be thrown.
     *
     * @return A Tuple containing the found key and value
     * @throws IOException
     * @throws NoSuchElementException When we have reached the beginning of the B-tree, or if 
     * the B-tree is empty
     */
    public Tuple<K, V> prev() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No more tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];

        switch( parentPos.pos )
        {
            case BEFORE_FIRST :
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );

            case AFTER_LAST :
                // Move the the last element
                parentPos.pos = parentPos.page.getNbElems() - 1;
                break;
            
            case 0 :
                // beginning of the leaf. We have to go back into the stack up to the
                // parent, and down to the leaf on the left, if possible
                parentPos = findPrevParentPos();

                // we also need to check for the type of page cause
                // findPrevParentPos will never return a null ParentPos
                if ( parentPos == null )
                {
                    // This is the end : no more value
                    throw new NoSuchElementException( "No more tuples present" );
                }
                else
                {
                    parentPos.pos--;
                }
                
                break;
                
            default :
                // Simply decrement the position
                parentPos.pos--;
                break;
        }

        // Get the value
        ValueHolder<V> valueHolder = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos );
        V value = valueHolder.get();

        // Get the key
        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
        K key = leaf.getKey( parentPos.pos );
        
        // Construct the Tuple 
        Tuple<K, V> tuple = new Tuple<K, V>( key, value );

        return tuple;
    }


    /**
     * Tells if there is a next ParentPos
     *
     * @return the new ParentPos instance, or null if we have no following leaf
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    private boolean hasNextParentPos() throws EndOfFileExceededException, IOException
    {
        if ( depth == 0 )
        {
            // No need to go any further, there is only one leaf in the btree
            return false;
        }

        int currentDepth = depth - 1;
        Page<K, V> child = null;

        // First, go up the tree until we find a Node which has some element on the right
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, V> parentPos = stack[currentDepth];

            if ( parentPos.pos + 1 > parentPos.page.getNbElems() )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos + 1 );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    child = ( ( AbstractPage<K, V> ) child ).getPage( 0 );
                }

                return true;
            }
        }

        return false;
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

            if ( parentPos.pos + 1 > parentPos.page.getNbElems() )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                parentPos.pos++;
                child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = 0;
                    parentPos.page = child;
                    child = ( ( AbstractPage<K, V> ) child ).getPage( 0 );
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.page = child;
                parentPos.pos = 0;

                return parentPos;
            }
        }

        return null;
    }


    /**
     * Find the leaf containing the previous elements. We are in
     * 
     * if the B-tree content is like :
     * <pre>
     *                    .
     *                   |
     *                   v
     *           +---+---+---+---+
     *           |///| X | Y |///|               Level N-1
     *           +---+---+---+---+
     *           |   |   |   |   |
     *   <-...---+   |   |   |   +---...-> 
     *               |   |   |
     *       <-...---+   |   |
     *                   |   |
     *         +---------+   +-------+
     *         |                     | 
     *         v                     v
     * +---+---+---+---+     +---+---+---+---+
     * |///|///| T |###|     | Y |///|///|///|   Level N
     * +---+---+---+---+     +---+---+---+---+
     *          pos           pos 
     *   New ParentPos       Current ParentPos
     * </pre>
     * 
     * and we are on Y, then the previous ParentPos instance will contain the page
     * on the left, teh position beoing on the last element of this page, T
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

        // Go up one step
        int currentDepth = depth - 1;
        Page<K, V> child = null;

        // First, go up the tree until we find a Node which has some element on the left
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the first one
            ParentPos<K, V> parentPos = stack[currentDepth];

            if ( parentPos.pos == 0 )
            {
                // No more element on the left : go up
                currentDepth--;
            }
            else
            {
                // We can pick the previous element at this level
                parentPos.pos--;
                child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos );

                // and go down the tree through the nodes, positioning on the rightmost element
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = child.getNbElems();
                    parentPos.page = child;
                    child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.page.getNbElems() );
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.pos = child.getNbElems() - 1;
                parentPos.page = child;

                return parentPos;
            }
        }

        return null;
    }


    /**
     * Tells if there is a prev ParentPos
     *
     * @return the new ParentPos instance, or null if we have no previous leaf
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    private boolean hasPrevParentPos() throws EndOfFileExceededException, IOException
    {
        if ( depth == 0 )
        {
            // No need to go any further, there is only one leaf in the btree
            return false;
        }

        int currentDepth = depth - 1;
        Page<K, V> child = null;

        // First, go up the tree until we find a Node which has some element on the right
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, V> parentPos = stack[currentDepth];

            if ( parentPos.pos == 0 )
            {
                // No more element on the left : go up
                currentDepth--;
            }
            else
            {
                // We can pick the previous element at this level
                child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos - 1 );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    child = ( ( AbstractPage<K, V> ) child ).getPage( child.getNbElems() );
                }

                return true;
            }
        }

        return false;
    }


    /**
     * Checks if this Cursor is closed. Calls to this operation should not
     * fail with exceptions if and only if the cursor is in the closed state.
     *
     * @return true if this Cursor is closed, false otherwise
     */
    public boolean isClosed()
    {
        return transaction.isClosed();
    }


    /**
     * Closes this Cursor and frees any resources it my have allocated.
     * Repeated calls to this method after this Cursor has already been
     * called should not fail with exceptions.
     */
    public void close()
    {
        transaction.close();
    }


    /**
     * Closes this Cursor and frees any resources it my have allocated.
     * Repeated calls to this method after this Cursor has already been
     * called should not fail with exceptions.  The reason argument is 
     * the Exception instance thrown instead of the standard 
     * CursorClosedException.
     *
     * @param reason exception thrown when this Cursor is accessed after close
     */
    public void close( Exception reason )
    {
        transaction.close();
    }


    /**
     * Gets the object at the current position.  Cursor implementations may
     * choose to reuse element objects by re-populating them on advances
     * instead of creating new objects on each advance.
     *
     * @return the object at the current position
     * @throws NoSuchElementException if the object at this Cursor's current position
     * cannot be retrieved, or if this Cursor is closed
     */
    public Tuple<K, V> get() throws NoSuchElementException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No tuple present" );
        }

        ParentPos<K, V> parentPos = stack[depth];
        
        if ( ( parentPos.pos != BEFORE_FIRST ) && ( parentPos.pos != AFTER_LAST ) )
        {
            // We have some element, build the Tuple
            // Get the value
            ValueHolder<V> valueHolder = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos );

            V value = valueHolder.get();

            // Get the key
            AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
            K key = leaf.getKey( parentPos.pos );
            
            // Build the Tuple
            Tuple<K, V> tuple = new Tuple<K, V>( key, value );

            return tuple;
        }
        
        throw new NoSuchElementException( "No element at this position : " + parentPos.pos );
    }


    /**
     * Get the creation date
     * @return The creation date for this cursor
     */
    public long getCreationDate()
    {
        return transaction.getCreationDate();
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
