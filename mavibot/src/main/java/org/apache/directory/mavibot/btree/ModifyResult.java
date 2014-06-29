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
 * The result of an insert operation, when the child has not been split. It contains the
 * reference to the modified page.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value

 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class ModifyResult<K, V> extends AbstractResult<K, V> implements InsertResult<K, V>
{
    /** The modified page reference */
    protected Page<K, V> modifiedPage;

    /** The modified value if the key was found in the tree*/
    protected V modifiedValue;


    /**
     * The default constructor for ModifyResult.
     * 
     * @param modifiedPage The modified page
     * @param modifiedvalue The modified value (can be null if the key wasn't present in the tree)
     */
    public ModifyResult( Page<K, V> modifiedPage, V modifiedValue )
    {
        super();
        this.modifiedPage = modifiedPage;
        this.modifiedValue = modifiedValue;
    }


    /**
     * A constructor for ModifyResult which takes a list of copied pages.
     * 
     * @param copiedPages the list of copied pages
     * @param modifiedPage The modified page
     * @param modifiedvalue The modified value (can be null if the key wasn't present in the tree)
     */
    public ModifyResult( List<Page<K, V>> copiedPages, Page<K, V> modifiedPage, V modifiedValue )
    {
        super( copiedPages );
        this.modifiedPage = modifiedPage;
        this.modifiedValue = modifiedValue;
    }


    /**
     * @return the modifiedPage
     */
    public Page<K, V> getModifiedPage()
    {
        return modifiedPage;
    }


    /**
     * Set the modified page
     * @param modifiedPage The new modified page
     */
    public void setModifiedPage( Page<K, V> modifiedPage )
    {
        this.modifiedPage = modifiedPage;
    }


    /**
     * @return the modifiedValue
     */
    public V getModifiedValue()
    {
        return modifiedValue;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append( "ModifyResult, old value = " ).append( modifiedValue );
        sb.append( ", modifiedPage = " ).append( modifiedPage );
        sb.append( super.toString() );

        return sb.toString();
    }
}
