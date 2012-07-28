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
 * The result of an insert operation, when the page has been split. It contains
 * the new pivotal value, plus the reference on the two new pages.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
/* No qualifier */class SplitResult<K, V> implements InsertResult<K, V>
{
    /** The left child */
    protected Page<K, V> leftPage;

    /** The right child */
    protected Page<K, V> rightPage;

    /** The key pivot */
    protected K pivot;


    /**
     * The default constructor for SplitResult.
     * @param pivot The new key to insert into the parent
     * @param leftPage The new left page
     * @param rightPage The new right page
     */
    /* No qualifier */SplitResult( K pivot, Page<K, V> leftPage, Page<K, V> rightPage )
    {
        this.pivot = pivot;
        this.leftPage = leftPage;
        this.rightPage = rightPage;
    }


    /**
     * @return the leftPage
     */
    /* No qualifier */Page<K, V> getLeftPage()
    {
        return leftPage;
    }


    /**
     * @return the rightPage
     */
    /* No qualifier */Page<K, V> getRightPage()
    {
        return rightPage;
    }


    /**
     * @return the pivot
     */
    /* No qualifier */K getPivot()
    {
        return pivot;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "SplitResult, new pivot = " ).append( pivot );
        sb.append( "\n    leftPage = " ).append( leftPage );
        sb.append( "\n    rightPage = " ).append( rightPage );

        return sb.toString();
    }
}
