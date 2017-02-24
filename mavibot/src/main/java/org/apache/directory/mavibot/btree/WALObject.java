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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A WALObject is an object stored in the TransactionContext before weing written on disk.
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface WALObject<K, V>
{
    /**
     * @return the B-tree
     */
    public BTree<K, V> getBtree();

    
    /**
     * @return The offset for this WALObject
     */
    long getOffset();
    
    
    /**
     * @return the underlying B-tree name
     */
    String getName();
    
    /**
     * @return The PageIOs associated with the Object
     */
    PageIO[] getPageIOs();
    
    
    /**
     * @param pageIOs Store teh PageIOs for this object
     */
    void setPageIOs( PageIO[] pageIOs );
    
    
    /**
     * Serialize a WALObject into PageIO[]
     * 
     * @param transaction The Write Transaction in use
     * @return The serialized WALObject into some PageIOs
     * 
     * @throws IOException If we got an error while serializing
     */
    PageIO[] serialize( WriteTransaction transaction ) throws IOException;
    
    
    /**
     * Deserialize a WALObject from a PageIO[]
     * 
     * @param transaction The Read Transaction in use
     * @param byteBuffer The byteBuffer containing the page data
     * @return The read page
     * 
     * @throws IOException If we got an error while serializing
     */
    WALObject<K, V> deserialize( Transaction transaction, ByteBuffer byteBuffer ) throws IOException;

    
    /**
     * @return the revision
     */
    long getRevision();
    
    
    /**
     * @return the Page id
     */
    long getId();
    
    
    /**
     * Print the content of this WALObject in a dense format
     * @return The pretty print for this instance
     */
    String prettyPrint();
}
