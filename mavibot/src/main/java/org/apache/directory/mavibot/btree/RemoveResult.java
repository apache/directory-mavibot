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


import java.util.List;


/**
 * The result of a delete operation, when the child has not been merged. It contains the
 * reference to the modified page, and the removed element.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value

 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class RemoveResult<K, V> extends AbstractDeleteResult<K, V>
{
    /**
     * The default constructor for RemoveResult.
     * 
     * @param modifiedPage The modified page
     * @param removedElement The removed element (can be null if the key wasn't present in the tree)
     */
    public RemoveResult( Page<K, V> modifiedPage, Tuple<K, V> removedElement )
    {
        super( modifiedPage, removedElement );
    }


    /**
     * A constructor for RemoveResult which takes a list of copied pages.
     * 
     * @param copiedPages the list of copied pages
     * @param modifiedPage The modified page
     * @param removedElement The removed element (can be null if the key wasn't present in the tree)
     */
    public RemoveResult( List<Page<K, V>> copiedPages, Page<K, V> modifiedPage, Tuple<K, V> removedElement )
    {
        super( copiedPages, modifiedPage, removedElement );
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "RemoveResult :" );
        sb.append( "\n    removed element = " ).append( getRemovedElement() );
        sb.append( "\n    modifiedPage = " ).append( getModifiedPage() );

        return sb.toString();
    }
}
