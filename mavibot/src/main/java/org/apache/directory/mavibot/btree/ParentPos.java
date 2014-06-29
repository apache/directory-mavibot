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
 * This class is used to store the parent page and the position in it during
 * a browse operation. We have as many ParentPos instance than the depth of the tree.
 * 
 * @param <K> The type for the Key
 * @param <V> The type for the stored value
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class ParentPos<K, V>
{
    /** The page we are browsing */
    /* no qualifier */Page<K, V> page;

    /** The current position in the page */
    /* no qualifier */int pos;

    /** The current position of the duplicate container in the page */
    /* no qualifier */int dupPos;

    /** The current position of the duplicate container in the page */
    /* no qualifier */ValueCursor<V> valueCursor;


    /**
     * Creates a new instance of ParentPos
     * @param page The current Page
     * @param pos The current position in the page
     */
    /* no qualifier */ParentPos( Page<K, V> page, int pos )
    {
        this.page = page;
        this.pos = pos;
    }


    /**
     * @see Object#toString()
     */
    public String toString()
    {
        return "<" + pos + "," + page + ">";
    }
}