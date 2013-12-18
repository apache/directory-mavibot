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
 * An abstract class to gather common elements of the DeleteResult
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/abstract class AbstractDeleteResult<K, V> extends AbstractResult<K, V> implements
    DeleteResult<K, V>
{
    /** The modified page reference */
    private Page<K, V> modifiedPage;

    /** The removed element if the key was found in the tree*/
    private Tuple<K, V> removedElement;


    /**
     * The default constructor for AbstractDeleteResult.
     * 
     * @param modifiedPage The modified page
     * @param removedElement The removed element (can be null if the key wasn't present in the tree)
     */
    /*no qualifier*/AbstractDeleteResult( Page<K, V> modifiedPage, Tuple<K, V> removedElement )
    {
        super();
        this.modifiedPage = modifiedPage;
        this.removedElement = removedElement;
    }


    /**
     * The default constructor for AbstractDeleteResult.
     * 
     * @param copiedPages the list of copied pages
     * @param modifiedPage The modified page
     * @param removedElement The removed element (can be null if the key wasn't present in the tree)
     */
    /*no qualifier*/AbstractDeleteResult( List<Page<K, V>> copiedPages, Page<K, V> modifiedPage,
        Tuple<K, V> removedElement )
    {
        super( copiedPages );
        this.modifiedPage = modifiedPage;
        this.removedElement = removedElement;
    }


    /**
     * {@inheritDoc}
     */
    public Page<K, V> getModifiedPage()
    {
        return modifiedPage;
    }


    /**
     * {@inheritDoc}
     */
    public Tuple<K, V> getRemovedElement()
    {
        return removedElement;
    }


    /**
     * @param modifiedPage the modifiedPage to set
     */
    /*no qualifier*/void setModifiedPage( Page<K, V> modifiedPage )
    {
        this.modifiedPage = modifiedPage;
    }
}
