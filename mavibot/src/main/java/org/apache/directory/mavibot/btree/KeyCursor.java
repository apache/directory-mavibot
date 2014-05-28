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
 * A Cursor is used to fetch only keys in a BTree and is returned by the
 * @see BTree#browseKeys method. The cursor <strng>must</strong> be closed
 * when the user is done with it.
 * <p>
 *
 * @param <K> The type for the Key
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class KeyCursor<K>
{
    /** A marker to tell that we are before the first element */
    private static final int BEFORE_FIRST = -1;

    /** A marker to tell that we are after the last element */
    private static final int AFTER_LAST = -2;

    /** The stack of pages from the root down to the leaf */
    protected ParentPos<K, K>[] stack;

    /** The stack's depth */
    protected int depth = 0;

    /** The transaction used for this cursor */
    protected ReadTransaction<K, K> transaction;


    /**
     * Creates a new instance of Cursor.
     */
    protected KeyCursor()
    {
    }


    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     *
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    public KeyCursor( ReadTransaction<K, K> transaction, ParentPos<K, K>[] stack, int depth )
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

        Page<K, K> child = null;

        for ( int i = 0; i < depth; i++ )
        {
            ParentPos<K, K> parentPos = stack[i];

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

            child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( parentPos.pos );
        }

        // and leaf
        ParentPos<K, K> parentPos = stack[depth];

        if ( child == null )
        {
            parentPos.pos = parentPos.page.getNbElems() - 1;
        }
        else
        {
            parentPos.page = child;
            parentPos.pos = child.getNbElems() - 1;
        }

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

        Page<K, K> child = null;

        for ( int i = 0; i < depth; i++ )
        {
            ParentPos<K, K> parentPos = stack[i];
            parentPos.pos = 0;

            if ( child != null )
            {
                parentPos.page = child;
            }

            child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( 0 );
        }

        // and leaf
        ParentPos<K, K> parentPos = stack[depth];
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

        // Take the leaf and check if we have no mare keys
        ParentPos<K, K> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // Empty BTree, get out
            return false;
        }

        if ( parentPos.pos == AFTER_LAST )
        {
            return false;
        }

        if ( parentPos.pos == BEFORE_FIRST )
        {
            return true;
        }

        if ( parentPos.pos < parentPos.page.getNbElems() - 1 )
        {
            // Not the last position, we have a next key
            return true;
        }
        else
        {
            // Ok, here, we have reached the last key in the leaf. We have to go up and
            // see if we have some remaining keys
            int currentDepth = depth - 1;

            while ( currentDepth >= 0 )
            {
                parentPos = stack[currentDepth];

                if ( parentPos.pos < parentPos.page.getNbElems() )
                {
                    // The parent has some remaining keys on the right, get out
                    return true;
                }
                else
                {
                    currentDepth--;
                }
            }

            // We are done, there are no more key left
            return false;
        }
    }


    /**
     * Find the next key
     *
     * @return the found key
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public K next() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No Key is present" );
        }

        ParentPos<K, K> parentPos = stack[depth];

        if ( ( parentPos.page == null ) || ( parentPos.pos == AFTER_LAST ) )
        {
            // This is the end : no more keys
            throw new NoSuchElementException( "No more keys present" );
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
                // This is the end : no more keys
                throw new NoSuchElementException( "No more keys present" );
            }
        }

        // Deal with the BeforeFirst case
        if ( parentPos.pos == BEFORE_FIRST )
        {
            parentPos.pos++;
        }
        else
        {
            if ( parentPos.pos == parentPos.page.getNbElems() - 1 )
            {
                parentPos = findNextParentPos();
                
                if ( ( parentPos == null ) || ( parentPos.page == null ) )
                {
                    // This is the end : no more keys
                    throw new NoSuchElementException( "No more keys present" );
                }
            }
            else
            {
                parentPos.pos++;
            }
        }
        
        AbstractPage<K, K> leaf = ( AbstractPage<K, K> ) ( parentPos.page );

        return leaf.getKey( parentPos.pos );
    }


    /**
     * Get the next key.
     */
    public K nextKey() throws EndOfFileExceededException, IOException
    {
        return next();
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

        ParentPos<K, K> parentPos = stack[depth];

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

        // Take the leaf and check if we have no mare keys
        ParentPos<K, K> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // Empty BTree, get out
            return false;
        }

        if ( parentPos.pos > 0 )
        {
            // get out, we have keys on the left
            return true;
        }
        else
        {
            // Check that we are not before the first key
            if ( parentPos.pos == BEFORE_FIRST )
            {
                return false;
            }

            if ( parentPos.pos == AFTER_LAST )
            {
                return true;
            }

            // Ok, here, we have reached the first key in the leaf. We have to go up and
            // see if we have some remaining keys
            int currentDepth = depth - 1;

            while ( currentDepth >= 0 )
            {
                parentPos = stack[currentDepth];

                if ( parentPos.pos > 0 )
                {
                    // The parent has some remaining keys on the right, get out
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
     * Find the previous key
     *
     * @return the found key
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    public K prev() throws EndOfFileExceededException, IOException
    {
        // First check that we have elements in the BTree
        if ( ( stack == null ) || ( stack.length == 0 ) )
        {
            throw new NoSuchElementException( "No more keys present" );
        }

        ParentPos<K, K> parentPos = stack[depth];

        if ( ( parentPos.page == null ) || ( parentPos.pos == BEFORE_FIRST ) )
        {
            // This is the end : no more keys
            throw new NoSuchElementException( "No more keys present" );
        }

        // Deal with the AfterLast case
        if ( parentPos.pos == AFTER_LAST )
        {
            parentPos.pos = parentPos.page.getNbElems() - 1;
        }
        else
        {
            if ( parentPos.pos == 0 )
            {
                parentPos = findPrevParentPos();
                
                if ( ( parentPos == null ) || ( parentPos.page == null ) )
                {
                    // This is the end : no more keys
                    throw new NoSuchElementException( "No more keys present" );
                }
            }
            else
            {
                parentPos.pos--;
            }
        }

        AbstractPage<K, K> leaf = ( AbstractPage<K, K> ) ( parentPos.page );

        return leaf.getKey( parentPos.pos );
    }


    /**
     * Get the previous key.
     * 
     * @return the found key
     * @throws EndOfFileExceededException
     * @throws IOException
     */
    public K prevKey() throws EndOfFileExceededException, IOException
    {
        return prev();
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

        ParentPos<K, K> parentPos = stack[depth];

        if ( parentPos.page == null )
        {
            // This is the end : no more key
            return false;
        }

        switch ( parentPos.pos )
        {
            case 0 :
                // Beginning of the leaf. We have to go back into the stack up to the
                // parent, and down to the leaf
                return hasPrevParentPos();
                
            case -1 :
                // no previous key
                return false;
                
            default :
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
        Page<K, K> child = null;

        // First, go up the tree until we find a Node which has some element on the right
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, K> parentPos = stack[currentDepth];

            if ( parentPos.pos + 1 > parentPos.page.getNbElems() )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( parentPos.pos + 1 );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    child = ( ( AbstractPage<K, K> ) child ).getPage( 0 );
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
    private ParentPos<K, K> findNextParentPos() throws EndOfFileExceededException, IOException
    {
        if ( depth == 0 )
        {
            // No need to go any further, there is only one leaf in the btree
            return null;
        }

        int currentDepth = depth - 1;
        Page<K, K> child = null;

        // First, go up the tree until we find a Node which has some element on the right
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, K> parentPos = stack[currentDepth];

            if ( parentPos.pos + 1 > parentPos.page.getNbElems() )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                parentPos.pos++;
                child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( parentPos.pos );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = 0;
                    parentPos.page = child;
                    child = ( ( AbstractPage<K, K> ) child ).getPage( 0 );
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
     * Find the leaf containing the previous elements.
     *
     * @return the new ParentPos instance, or null if we have no previous leaf
     * @throws IOException
     * @throws EndOfFileExceededException
     */
    private ParentPos<K, K> findPrevParentPos() throws EndOfFileExceededException, IOException
    {
        if ( depth == 0 )
        {
            // No need to go any further, there is only one leaf in the btree
            return null;
        }

        int currentDepth = depth - 1;
        Page<K, K> child = null;

        // First, go up the tree until we find a Node which has some element on the left
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, K> parentPos = stack[currentDepth];

            if ( parentPos.pos == 0 )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                parentPos.pos--;
                child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( parentPos.pos );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = child.getNbElems();
                    parentPos.page = child;
                    child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( parentPos.page.getNbElems() );
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
        Page<K, K> child = null;

        // First, go up the tree until we find a Node which has some element on the right
        while ( currentDepth >= 0 )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, K> parentPos = stack[currentDepth];

            if ( parentPos.pos == 0 )
            {
                // No more element on the left : go up
                currentDepth--;
            }
            else
            {
                // We can pick the previous element at this level
                child = ( ( AbstractPage<K, K> ) parentPos.page ).getPage( parentPos.pos - 1 );

                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    child = ( ( AbstractPage<K, K> ) child ).getPage( child.getNbElems() );
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

        sb.append( "KeyCursor, depth = " ).append( depth ).append( "\n" );

        for ( int i = 0; i <= depth; i++ )
        {
            sb.append( "    " ).append( stack[i] ).append( "\n" );
        }

        return sb.toString();
    }
}
