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
package org.apache.directory.mavibot.btree.persisted;


import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.directory.mavibot.btree.AbstractPage;
import org.apache.directory.mavibot.btree.AbstractTupleCursor;
import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.Page;
import org.apache.directory.mavibot.btree.ParentPos;
import org.apache.directory.mavibot.btree.Transaction;
import org.apache.directory.mavibot.btree.Tuple;
import org.apache.directory.mavibot.btree.ValueHolder;
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
public class TupleCursorImpl<K, V> extends AbstractTupleCursor<K, V>
{
    /** The BTree we are walking */
    protected BTree<K, V> btree;
    

    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     * 
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    TupleCursorImpl( BTree<K, V> btree, Transaction<K, V> transaction, ParentPos<K, V>[] stack, int depth )
    {
        super( transaction, stack, depth );

        this.btree = btree;
    }


    /**
     * {@inheritDoc}
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
                ValueHolder<V> valueHolder = ( ( PersistedLeaf<K, V> ) parentPos.page ).getValue( parentPos.pos );
                
                parentPos.valueCursor = valueHolder.getCursor();
                
                value = parentPos.valueCursor.next();
            }
            catch ( IllegalArgumentException e )
            {
                e.printStackTrace();
            }
        }
        
        PersistedLeaf<K, V> leaf = ( PersistedLeaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.getKey( parentPos.pos ) );
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

            if ( parentPos.pos + 1 > parentPos.page.getNbElems() )
            {
                // No more element on the right : go up
                currentDepth--;
            }
            else
            {
                // We can pick the next element at this level
                parentPos.pos++;
                child = ((AbstractPage<K, V>)parentPos.page).getPage( parentPos.pos );
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = 0;
                    parentPos.page = child;
                    child = ((AbstractPage<K, V>)child).getPage( 0 );
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.page = child;
                parentPos.pos = 0;
                parentPos.valueCursor = ((PersistedLeaf<K, V>)child).values[0].getCursor();

                return parentPos;
            }
        }

        return null;
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
                child = ((AbstractPage<K, V>)parentPos.page).getPage( parentPos.pos + 1 );
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    child = ((AbstractPage<K, V>)child).getPage( 0 );
                }

                return true;
            }
        }

        return false;
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
                child = ((AbstractPage<K, V>)parentPos.page).getPage( parentPos.pos - 1 );
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    child = ((AbstractPage<K, V>)child).getPage( child.getNbElems() );
                }

                return true;
            }
        }

        return false;
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
                child = ((AbstractPage<K, V>)parentPos.page).getPage( parentPos.pos );
                
                // and go down the tree through the nodes
                while ( currentDepth < depth - 1 )
                {
                    currentDepth++;
                    parentPos = stack[currentDepth];
                    parentPos.pos = child.getNbElems();
                    parentPos.page = child;
                    child = ((AbstractPage<K, V>)parentPos.page).getPage( parentPos.page.getNbElems() );
                }

                // and the leaf
                parentPos = stack[depth];
                parentPos.pos = child.getNbElems() - 1;
                parentPos.page = child;
                ValueHolder<V> valueHolder = ((PersistedLeaf<K, V>)parentPos.page).values[parentPos.pos];
                parentPos.valueCursor = valueHolder.getCursor();
                parentPos.valueCursor.afterLast();

                return parentPos;
            }
        }
        
        return null;
    }


    /**
     * {@inheritDoc}
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
            if ( ( parentPos == null ) || ( parentPos.page == null ) || ( parentPos.page instanceof Node ) )
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
                    ValueHolder<V> valueHolder = ( ( PersistedLeaf<K, V> ) parentPos.page ).getValue( parentPos.pos );
                    
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


        PersistedLeaf<K, V> leaf = ( PersistedLeaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.getKey( parentPos.pos ) );
        tuple.setValue( value );

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
        
        // Take the leaf and check if we have no mare values
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
            return hasNextParentPos();
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
            ParentPos<K, V> newParentPos = findNextParentPos();

            // we also need to check the result of the call to
            // findNextParentPos as it will return a null ParentPos
            if ( ( newParentPos == null ) || ( newParentPos.page == null ) )
            {
                // This is the end : no more value
                PersistedLeaf<K, V> leaf = ( PersistedLeaf<K, V> ) ( parentPos.page );
                ValueHolder<V> valueHolder = leaf.values[parentPos.pos];
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
        PersistedLeaf<K, V> leaf = ( PersistedLeaf<K, V> ) ( parentPos.page );
        tuple.setKey( leaf.getKey( parentPos.pos ) );
        
        // The value
        ValueHolder<V> valueHolder = leaf.values[parentPos.pos];
        parentPos.valueCursor = valueHolder.getCursor();
        V value = parentPos.valueCursor.next();
        tuple.setValue( value );

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
            return hasPrevParentPos();
        }
        else
        {
            return true;
        }
    }


    /**
     * {@inheritDoc}
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
        PersistedLeaf<K, V> leaf = ( PersistedLeaf<K, V> ) ( parentPos.page );

        // The key
        tuple.setKey( leaf.getKey( parentPos.pos ) );

        // The value
        ValueHolder<V> valueHolder = leaf.values[parentPos.pos];
        parentPos.valueCursor = valueHolder.getCursor();
        V value = parentPos.valueCursor.next();
        tuple.setValue( value );
        
        return tuple;
    }


}