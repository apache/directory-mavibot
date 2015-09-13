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


import java.util.ArrayList;
import java.util.List;


/**
 * An abstract class to gather common elements of the Result classes
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/abstract class AbstractResult<K, V> implements Result<Page<K, V>>
{
    /** The list of copied page reference */
    private List<Page<K, V>> copiedPages;


    /**
     * The default constructor for AbstractResult.
     * 
     */
    public AbstractResult()
    {
        copiedPages = new ArrayList<Page<K, V>>();
    }


    /**
     * Creates an instance of AbstractResult with an initialized list of copied pages.
     * 
     * @param copiedPages The list of copied pages to store in this result
     */
    public AbstractResult( List<Page<K, V>> copiedPages )
    {
        this.copiedPages = copiedPages;
    }


    /**
     * {@inheritDoc}
     */
    public List<Page<K, V>> getCopiedPages()
    {
        return copiedPages;
    }


    /**
     * {@inheritDoc}
     */
    public void addCopiedPage( Page<K, V> page )
    {
        copiedPages.add( page );
    }


    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "\n    copiedPage = <" );

        boolean isFirst = true;

        for ( Page<K, V> copiedPage : getCopiedPages() )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                sb.append( ", " );
            }

            sb.append( ( ( AbstractPage<K, V> ) copiedPage ).getOffset() );
        }

        sb.append( ">" );

        return sb.toString();
    }
}
