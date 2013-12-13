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
public abstract class AbstractTupleCursor<K, V> implements TupleCursor<K, V>
{
    /** The stack of pages from the root down to the leaf */
    protected ParentPos<K, V>[] stack;
    
    /** The stack's depth */
    protected int depth = 0;

    /** The transaction used for this cursor */
    protected Transaction<K, V> transaction;

    /** The Tuple used to return the results */
    protected Tuple<K, V> tuple = new Tuple<K, V>();

    /**
     * Creates a new instance of Cursor, starting on a page at a given position.
     * 
     * @param transaction The transaction this operation is protected by
     * @param stack The stack of parent's from root to this page
     */
    protected AbstractTupleCursor( Transaction<K, V> transaction, ParentPos<K, V>[] stack, int depth )
    {
        this.transaction = transaction;
        this.stack = stack;
        this.depth = depth;
    }
    
    
    /**
     * {@inheritDoc}
     *
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

            child = ((Node<K, V>)parentPos.page).children[parentPos.pos];
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

        parentPos.valueCursor = ((Leaf<K, V>)parentPos.page).values[parentPos.pos].getCursor();
        parentPos.valueCursor.afterLast();
        parentPos.pos = AFTER_LAST;
    }


    /**
     * {@inheritDoc}
     */
    public void close()
    {
        transaction.close();
    }


    /**
     * {@inheritDoc}
     */
    public long getCreationDate()
    {
        return transaction.getCreationDate();
    }


    /**
     * {@inheritDoc}
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
