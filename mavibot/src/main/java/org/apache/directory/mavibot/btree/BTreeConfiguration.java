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


import org.apache.directory.mavibot.btree.serializer.ElementSerializer;


/**
 * The B+Tree Configuration. This class can be used to store all the configurable
 * parameters used by the B-tree class
 *
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTreeConfiguration<K, V>
{
    /** Number of entries in each Page. */
    private int pageNbElem = BTree.DEFAULT_PAGE_NBELEM;

    /** The Key and Value serializer used for this tree. If none is provided,
     * the B-tree will deduce the serializer to use from the generic type, and
     * use the default Java serialization  */
    private ElementSerializer<K> keySerializer;
    private ElementSerializer<V> valueSerializer;

    /** The B-tree name */
    private String name;

    /** The B-tree type */
    private BTreeTypeEnum btreeType = BTreeTypeEnum.PERSISTED;


    /**
     * @return the number of elements per page
     */
    public int getPageNbElem()
    {
        return pageNbElem;
    }


    /**
     * @param pageNbElem the number of elements per page to set
     */
    public void setPageNbElem( int pageNbElem )
    {
        this.pageNbElem = pageNbElem;
    }


    /**
     * @return the key serializer
     */
    public ElementSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }


    /**
     * @return the value serializer
     */
    public ElementSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }


    /**
     * @param keySerializer the key serializer to set
     * @param valueSerializer the value serializer to set
     */
    public void setSerializers( ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer )
    {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }


    /**
     * @param serializer the key serializer to set
     */
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        this.keySerializer = keySerializer;
    }


    /**
     * @param serializer the key serializer to set
     */
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
    }


    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }


    /**
     * @param name the name to set
     */
    public void setName( String name )
    {
        this.name = name.trim();
    }


    /**
     * @return The BtreeType for this Btree
     */
    public BTreeTypeEnum getBtreeType()
    {
        return btreeType;
    }


    /**
     * @param btreeType The BtreeType
     */
    public void setBtreeType( BTreeTypeEnum btreeType )
    {
        this.btreeType = btreeType;
    }
}
