/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */

package org.apache.directory.mavibot.btree.memory;


import java.io.IOException;

import org.apache.directory.mavibot.btree.TupleCursor;


/**
 * A class containing utility methods to be used internally. 
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
@SuppressWarnings("all")
/* No qualifier */class InternalUtil
{

    /**
     * Sets the multi-value container(a.k.a valueCursor) of the key at the given position.
     * 
     * This method will not update the existing value of 'dupsPos'. To change this value
     * use {@link #changeNextDupsContainer(ParentPos, BTree)} or {@link #changePrevDupsContainer(ParentPos, BTree)}
     *  
     * @param parentPos the parent position object
     * @param btree the BTree
     */
    public static void setDupsContainer( ParentPos parentPos, BTree btree )
    {
        if ( !btree.isAllowDuplicates() )
        {
            return;
        }

        if ( parentPos.valueCursor == null )
        {
            Leaf leaf = ( Leaf ) ( parentPos.page );
            
            // Deal with BEFORE_FIRST and AFTER_LAST cases
            if ( ( parentPos.pos == TupleCursor.BEFORE_FIRST ) || ( parentPos.pos == TupleCursor.AFTER_LAST ) )
            {
                // Nothing to do in this case
                return;
            }
            else
            {
                MultipleMemoryHolder mvHolder = ( MultipleMemoryHolder ) leaf.values[parentPos.pos];
                
                if ( !mvHolder.isSingleValue() )
                {
                    BTree valueCursor = ( BTree ) mvHolder.getValue();
                    parentPos.valueCursor = valueCursor;
                }
            }
        }
    }


    /**
     * Sets the multi-value container(a.k.a valueCursor) of the key at the given position
     * and resets the 'dupsPos' to zero. This is mostly used by Cursor while navigating using
     * next() 
     *
     * @param parentPos the parent position object
     * @param btree the BTree
     */
    public static void changeNextDupsContainer( ParentPos parentPos, BTree btree ) throws IOException
    {
        if ( !btree.isAllowDuplicates() )
        {
            return;
        }

        if ( parentPos.pos < parentPos.page.getNbElems() )
        {
            Leaf leaf = ( Leaf ) ( parentPos.page );
            MultipleMemoryHolder mvHolder = ( MultipleMemoryHolder ) leaf.values[parentPos.pos];
            if( !mvHolder.isSingleValue() )
            {
                BTree valueCursor = ( BTree ) mvHolder.getValue();
                parentPos.valueCursor = valueCursor;
                parentPos.dupPos = 0;
            }
        }
    }


    /**
     * Sets the multi-value container(a.k.a valueCursor) of the key at the index below the given position( i.e pos - 1)
     * and resets the 'dupsPos' to the number of elements present in the multi-value container.
     * This is used by Cursor while navigating using prev() 
     *
     * @param parentPos the parent position object
     * @param btree the BTree
     */
    public static void changePrevDupsContainer( ParentPos parentPos, BTree btree ) throws IOException
    {
        if ( !btree.isAllowDuplicates() )
        {
            return;
        }

        int index = parentPos.pos - 1;
        if ( index >= 0 )
        {
            Leaf leaf = ( Leaf ) ( parentPos.page );
            MultipleMemoryHolder mvHolder = ( MultipleMemoryHolder ) leaf.values[index];
            if( !mvHolder.isSingleValue() )
            {
                parentPos.valueCursor = ( BTree ) mvHolder.getValue();
                parentPos.dupPos = ( int ) parentPos.valueCursor.getNbElems();
            }
            else
            {
                parentPos.valueCursor = null;
                parentPos.dupPos = -1;
            }
        }
    }


    /**
     * Same as @see #changePrevDupsContainer(ParentPos, BTree) but with a different name
     * to make it sound semantically right when used inside {@link BTreeFactory#getPathToRightMostLeaf(BTree)} 
     */
    public static void setLastDupsContainer( ParentPos parentPos, BTree btree ) throws IOException
    {
        changePrevDupsContainer( parentPos, btree );
    }
}
