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
import java.util.LinkedList;
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
    private LinkedList<ParentPos<K, V>> stack;

    /** The BTree we are walking */
    private BTree<K, V> btree;

    private boolean allowDuplicates;

    /** a copy of the stack given at the time of initializing the cursor. This is used for moving the cursor to start position */
    private LinkedList<ParentPos<K, V>> _initialStack;


    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     * 
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    TupleCursorImpl( BTree<K, V> btree, Transaction<K, V> transaction, LinkedList<ParentPos<K, V>> stack )
    {
        this.transaction = transaction;
        this.stack = stack;
        this.btree = btree;
        this.allowDuplicates = btree.isAllowDuplicates();

        _initialStack = new LinkedList<ParentPos<K, V>>();

        cloneStack( stack, _initialStack );
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
        ParentPos<K, V> parentPos = stack.getFirst();

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
        if ( parentPos.pos < 0 )
        {
            parentPos.pos = 0;
        }

        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.keys[parentPos.pos].getKey() );

        ValueHolder<V> valueHolder = leaf.values[parentPos.pos];
        tuple.setValue( valueHolder.getCursor().next() );
        parentPos.pos++;

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
        ParentPos<K, V> lastParentPos = null;

        while ( true )
        {
            // We first go up the tree, until we reach a page whose current position
            // is not the last one
            ParentPos<K, V> parentPos = stack.peek();

            if ( parentPos == null )
            {
                stack.push( lastParentPos );
                return lastParentPos;
            }

            if ( parentPos.pos == parentPos.page.getNbElems() )
            {
                lastParentPos = stack.pop();
                continue;
            }
            else
            {
                // Then we go down the tree until we find a leaf which position is not the last one.
                int newPos = ++parentPos.pos;
                ParentPos<K, V> newParentPos = parentPos;

                while ( newParentPos.page instanceof Node )
                {
                    Node<K, V> node = ( Node<K, V> ) newParentPos.page;

                    newParentPos = new ParentPos<K, V>( node.children[newPos].getValue( btree ), 0 );

                    stack.push( newParentPos );

                    newPos = 0;
                }

                if ( allowDuplicates )
                {
                    changeNextDupsContainer( newParentPos, btree );
                }

                return newParentPos;
            }
        }
    }


    /**
     * Find the leaf containing the previous elements.
     * 
     * @return the new ParentPos instance, or null if we have no previous leaf
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    private ParentPos<K, V> findPreviousParentPos() throws EndOfFileExceededException, IOException
    {
        ParentPos<K, V> lastParentPos = null;

        while ( true )
        {
            // We first go up the tree, until we reach a page which current position
            // is not the first one
            ParentPos<K, V> parentPos = stack.peek();

            if ( parentPos == null )
            {
                stack.push( lastParentPos );
                return lastParentPos;
            }

            if ( parentPos.pos == 0 )
            {
                lastParentPos = stack.pop();
                continue;
            }
            else
            {
                // Then we go down the tree until we find a leaf which position is not the first one.
                int newPos = --parentPos.pos;
                ParentPos<K, V> newParentPos = parentPos;

                while ( newParentPos.page instanceof Node )
                {
                    Node<K, V> node = ( Node<K, V> ) newParentPos.page;

                    newParentPos = new ParentPos<K, V>( node.children[newPos].getValue( btree ), node.children[newPos]
                        .getValue( btree ).getNbElems() );

                    stack.push( newParentPos );

                    newPos = node.getNbElems();
                }

                if ( allowDuplicates )
                {
                    changePrevDupsContainer( newParentPos, btree );
                }

                return newParentPos;
            }
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
        ParentPos<K, V> parentPos = stack.peek();

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            throw new NoSuchElementException( "No more tuples present" );
        }

        if ( parentPos.pos == 0 && parentPos.dupPos == 0 )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPreviousParentPos();

            // we also need to check for the type of page cause
            // findPrevParentPos will never return a null ParentPos
            if ( parentPos.page == null || ( parentPos.page instanceof Node ) )
            {
                // This is the end : no more value
                throw new NoSuchElementException( "No more tuples present" );
            }
        }

        Leaf<K, V> leaf = ( Leaf<K, V> ) ( parentPos.page );
        ValueHolder<V> valueHolder = leaf.values[parentPos.pos];
        tuple.setKey( leaf.keys[parentPos.pos].getKey() );
        tuple.setValue( valueHolder.getCursor().next() );

        return tuple;
    }


    /**
     * Tells if the cursor can return a next element
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasNext() throws EndOfFileExceededException, IOException
    {
        ParentPos<K, V> parentPos = stack.peek();

        if ( parentPos.page == null )
        {
            return false;
        }

        for ( ParentPos<K, V> p : stack )
        {
            if ( allowDuplicates && ( p.page instanceof Leaf ) )
            {
                if ( ( p.dupsContainer == null ) && ( p.pos != p.page.getNbElems() ) )
                {
                    return true;
                }
                else if ( ( p.dupsContainer != null ) && ( p.dupPos != p.dupsContainer.getNbElems() )
                    && ( p.pos != p.page.getNbElems() ) )
                {
                    return true;
                }
            }
            else if ( p.pos != p.page.getNbElems() )
            {
                return true;
            }
        }

        return false;
    }


    /**
     * Tells if the cursor can return a previous element
     * @return true if there are some more elements
     * @throws IOException 
     * @throws EndOfFileExceededException 
     */
    public boolean hasPrev() throws EndOfFileExceededException, IOException
    {
        ParentPos<K, V> parentPos = stack.peek();

        if ( parentPos.page == null )
        {
            return false;
        }

        for ( ParentPos<K, V> p : stack )
        {
            if ( allowDuplicates && ( p.page instanceof Leaf ) )
            {
                if ( ( p.dupsContainer == null ) && ( p.pos != 0 ) )
                {
                    return true;
                }
                else if ( ( p.dupsContainer != null ) &&
                    ( ( p.dupPos != 0 ) || ( p.pos != 0 ) ) )
                {
                    return true;
                }
            }
            else if ( p.pos != 0 )
            {
                return true;
            }
        }

        return false;
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
        ParentPos<K, V> parentPos = stack.getFirst();

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
        ParentPos<K, V> parentPos = stack.peek();

        if ( parentPos.page == null )
        {
            // This is the end : no more value
            return;
        }

        if ( parentPos.pos == 0 )
        {
            // End of the leaf. We have to go back into the stack up to the
            // parent, and down to the leaf
            parentPos = findPreviousParentPos();

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
        cloneStack( _initialStack, stack );
    }


    /**
     * Places the cursor at the end of the last position
     * 
     * @throws IOException
     */
    public void afterLast() throws IOException
    {
        stack.clear();
        stack = BTreeFactory.getPathToRightMostLeaf( btree );
    }


    /**
     * clones the original stack of ParentPos objects
     * 
     * @param original the original stack
     * @param clone the stack where the cloned ParentPos objects to be copied
     */
    private void cloneStack( LinkedList<ParentPos<K, V>> original, LinkedList<ParentPos<K, V>> clone )
    {
        clone.clear();

        // preserve the first position
        for ( ParentPos<K, V> o : original )
        {
            ParentPos<K, V> tmp = new ParentPos<K, V>( o.page, o.pos );
            tmp.dupPos = o.dupPos;
            tmp.dupsContainer = o.dupsContainer;
            clone.add( tmp );
        }
    }

}
