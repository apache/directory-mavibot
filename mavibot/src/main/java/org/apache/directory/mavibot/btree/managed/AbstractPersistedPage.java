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


import java.io.IOException;
import java.lang.reflect.Array;

import org.apache.directory.mavibot.btree.AbstractPage;
import org.apache.directory.mavibot.btree.BTree;
import org.apache.directory.mavibot.btree.KeyHolder;
import org.apache.directory.mavibot.btree.Page;


/**
 * A MVCC abstract Page. It stores the field and the methods shared by the Node and Leaf
 * classes.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier */abstract class AbstractPersistedPage<K, V> extends AbstractPage<K, V>
{
    /**
     * Creates a default empty AbstractPage
     * 
     * @param btree The associated BTree
     */
    protected AbstractPersistedPage( BTree<K, V> btree )
    {
        super( btree );
    }


    /**
     * Internal constructor used to create Page instance used when a page is being copied or overflow
     */
    @SuppressWarnings("unchecked")
    // Cannot create an array of generic objects
    protected AbstractPersistedPage( BTree<K, V> btree, long revision, int nbElems )
    {
        super( btree );
        this.revision = revision;
        this.nbElems = nbElems;
        this.keys = ( KeyHolder[] ) Array.newInstance( KeyHolder.class, nbElems );
    }


    /**
     * Selects the sibling (the previous or next page with the same parent) which has
     * the more element assuming it's above N/2
     * 
     * @param parent The parent of the current page
     * @param The position of the current page reference in its parent
     * @return The position of the sibling, or -1 if we have'nt found any sibling
     * @throws IOException If we have an error while trying to access the page
     */
    protected int selectSibling( Node<K, V> parent, int parentPos ) throws IOException
    {
        if ( parentPos == 0 )
        {
            // The current page is referenced on the left of its parent's page :
            // we will not have a previous page with the same parent
            return 1;
        }

        if ( parentPos == parent.getNbElems() )
        {
            // The current page is referenced on the right of its parent's page :
            // we will not have a next page with the same parent
            return parentPos - 1;
        }

        Page<K, V> prevPage = parent.children[parentPos - 1].getValue( btree );
        Page<K, V> nextPage = parent.children[parentPos + 1].getValue( btree );

        int prevPageSize = prevPage.getNbElems();
        int nextPageSize = nextPage.getNbElems();

        if ( prevPageSize >= nextPageSize )
        {
            return parentPos - 1;
        }
        else
        {
            return parentPos + 1;
        }
    }


    /**
     * Sets the key at a give position
     * 
     * @param pos The position in the keys array
     * @param key the key to inject
     */
    /* No qualifier*/void setKey( int pos, K key )
    {
        keys[pos] = new PersistedKeyHolder<K>( btree.getKeySerializer(), key );
    }


    /**
     * Sets the key at a give position
     * 
     * @param pos The position in the keys array
     * @param buffer the serialized key to inject
     */
    /* No qualifier*/void setKey( int pos, byte[] buffer )
    {
        keys[pos] = new PersistedKeyHolder<K>( btree.getKeySerializer(), buffer );
    }
}
