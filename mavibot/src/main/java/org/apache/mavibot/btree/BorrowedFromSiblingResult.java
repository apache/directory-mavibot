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
package org.apache.mavibot.btree;

/**
 * The result of a delete operation, when the child has not been merged. It contains the
 * reference to the modified page, and the removed element.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value

 * @author <a href="mailto:labs@laps.apache.org">Mavibot labs Project</a>
 */
/* No qualifier */ class BorrowedFromSiblingResult<K, V> implements DeleteResult<K, V>
{
    /** The modified page reference */
    protected Page<K, V> modifiedPage;
    
    /** The modified sibling reference */
    protected Page<K, V> modifiedSibling;
    
    /** The removed element if the key was found in the tree*/
    protected Tuple<K, V> removedElement;
    
    /** The new leftmost element if the removed k was on position 0. Null otherwise */
    protected K newLeftMost;
    
    /**
     * The default constructor for RemoveResult.
     * 
     * @param modifiedPage The modified page
     * @param
     * @param removedElement The removed element (can be null if the key wasn't present in the tree)
     */
    public BorrowedFromSiblingResult( Page<K, V> modifiedPage, Page<K, V> modifiedSibling, Tuple<K, V> removedElement, K newLeftMost )
    {
        this.modifiedPage = modifiedPage;
        this.modifiedSibling = modifiedSibling;
        this.removedElement = removedElement;
        this.newLeftMost = newLeftMost;
    }
    

    /**
     * @return the modifiedPage
     */
    public Page<K, V> getModifiedPage()
    {
        return modifiedPage;
    }
    

    /**
     * @return the modifiedSibling
     */
    public Page<K, V> getModifiedSibling()
    {
        return modifiedSibling;
    }


    /**
     * @return the removed element
     */
    public Tuple<K, V> getRemovedElement()
    {
        return removedElement;
    }


    /**
     * @return the newLeftMost
     */
    public K getNewLeftMost()
    {
        return newLeftMost;
    }
    
    
    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "RemoveResult, removed element = " ).append( removedElement );
        sb.append( ", modifiedPage = " ).append( modifiedPage );
        sb.append( ", new LeftMost = " ).append( newLeftMost );

        return sb.toString();
    }
}
