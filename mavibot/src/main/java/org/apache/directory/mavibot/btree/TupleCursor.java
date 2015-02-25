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
public class TupleCursor<K, V>
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
     * Change the position in the current cursor to set it after the last key
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
                parentPos.pos = child.getNbElems();
            }
            else
            {
                // We have N+1 children if the page is a Node, so we don't decrement the nbElems field
                parentPos.pos = parentPos.page.getNbElems();
            }

            child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos );
        }

        // and leaf
        ParentPos<K, V> parentPos = stack[depth];

        if ( child == null )
        {
            parentPos.pos = parentPos.page.getNbElems() - 1;
        }
        else
        {
            parentPos.page = child;
            parentPos.pos = child.getNbElems() - 1;
        }

        parentPos.valueCursor = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos ).getCursor();
        parentPos.valueCursor.afterLast();
        parentPos.pos = AFTER_LAST;
    }


    /**
     * Change the position in the current cursor before the first key
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

            child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( 0 );
        }

        // and leaf
        ParentPos<K, V> parentPos = stack[depth];
        parentPos.pos = BEFORE_FIRST;

        if ( child != null )
        {
            parentPos.page = child;
        }

        if ( parentPos.valueCursor != null )
        {
            parentPos.valueCursor = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( 0 ).getCursor();
            parentPos.valueCursor.beforeFirst();
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
            // Check if we have some more value
            if ( ( parentPos.valueCursor != null ) && parentPos.valueCursor.hasNext() )
            {
                // No problem, we still have some values to read
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
                    currentDepth--;
                }
            }

            // We are done, there are no more value left
            return false;
        }
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

        if ( ( parentPos.page == null ) || ( parentPos.pos == AFTER_LAST ) )
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
            // Deal with the BeforeFirst case
            if ( parentPos.pos == BEFORE_FIRST )
            {
                parentPos.pos++;
            }

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
                ValueHolder<V> valueHolder = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos );

                parentPos.valueCursor = valueHolder.getCursor();

                value = parentPos.valueCursor.next();
            }
            catch ( IllegalArgumentException e )
            {
                e.printStackTrace();
            }
        }

        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
        Tuple<K, V> tuple = new Tuple<K, V>( leaf.getKey( parentPos.pos ), value );

        return tuple;
    }


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
            ParentPos<K, V> newParentPos = findNextParentPos();

            // we also need to check the result of the call to
            // findNextParentPos as it will return a null ParentPos
            if ( ( newParentPos == null ) || ( newParentPos.page == null ) )
            {
                // This is the end : no more value
                AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
                ValueHolder<V> valueHolder = leaf.getValue( parentPos.pos );
                parentPos.pos = AFTER_LAST;
                parentPos.valueCursor = valueHolder.getCursor();
                parentPos.valueCursor.afterLast();

                return null;
            }
            else
            {
                parentPos = newParentPos;
            }
        }
        else
        {
            // Get the next key
            parentPos.pos++;
        }

        // The key
        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
        Tuple<K, V> tuple = new Tuple<K, V>();
        tuple.setKey( leaf.getKey( parentPos.pos ) );

        // The value
        ValueHolder<V> valueHolder = leaf.getValue( parentPos.pos );
        parentPos.valueCursor = valueHolder.getCursor();
        V value = parentPos.valueCursor.next();
        tuple.setValue( value );

        return tuple;
    }


    /**
     * Tells if the cursor can return a next key
     *
     * @return true if there are some more keys
     * @throws IOException
     * @throws EndOfFileExceededException
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
            return hasNextParentPos();
        }
        else
        {
            return true;
        }
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
                    currentDepth--;
                }
            }

            return false;
        }
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

        if ( ( parentPos.page == null ) || ( parentPos.pos == BEFORE_FIRST ) )
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
            if ( ( parentPos == null ) || ( parentPos.page == null ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }

        V value = null;

        if ( parentPos.valueCursor.hasPrev() )
        {
            // Deal with the AfterLast case
            if ( parentPos.pos == AFTER_LAST )
            {
                parentPos.pos = parentPos.page.getNbElems() - 1;
            }

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
                    ValueHolder<V> valueHolder = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos );

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

        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );
        Tuple<K, V> tuple = new Tuple<K, V>( leaf.getKey( parentPos.pos ), value );

        return tuple;
    }


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
        AbstractPage<K, V> leaf = ( AbstractPage<K, V> ) ( parentPos.page );

        // The key
        Tuple<K, V> tuple = new Tuple<K, V>();
        tuple.setKey( leaf.getKey( parentPos.pos ) );

        // The value
        ValueHolder<V> valueHolder = leaf.getValue( parentPos.pos );
        parentPos.valueCursor = valueHolder.getCursor();
        V value = parentPos.valueCursor.next();
        tuple.setValue( value );

        return tuple;
    }


    /**
     * Tells if the cursor can return a previous key
     *
     * @return true if there are some more keys
     * @throws IOException
     * @throws EndOfFileExceededException
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

        switch ( parentPos.pos )
        {
            case 0:
                // Beginning of the leaf. We have to go back into the stack up to the
                // parent, and down to the leaf
                return hasPrevParentPos();

            case -1:
                // no previous key
                return false;

            default:
                // we have a previous key
                return true;
        }
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
                parentPos.valueCursor = ( ( AbstractPage<K, V> ) child ).getValue( 0 ).getCursor();

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
                child = ( ( AbstractPage<K, V> ) parentPos.page ).getPage( parentPos.pos );

                // and go down the tree through the nodes
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
                ValueHolder<V> valueHolder = ( ( AbstractPage<K, V> ) parentPos.page ).getValue( parentPos.pos );
                parentPos.valueCursor = valueHolder.getCursor();
                parentPos.valueCursor.afterLast();

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
     * {@inheritDoc}
     */
    public void close()
    {
        transaction.close();
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
