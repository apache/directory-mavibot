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

/**
 * A WALObject is an object stored in a {@link Transaction} before being written on disk. 
 * <p>
 * The <strong>WAL</strong> (aka Write Ahead Log) is a temporary storage where updates are
 * kept until the transaction is committed on disk, or rolled back.
 * <p> 
 * The following elements are WalObjects :
 * <ul>
 *   <li>Leaf</li>
 *   <li>Node</li>
 *   <li>BtreeHeader</li>
 *   <li>BtreeInfo</li>
 * </ul>
 * <p>
 * In order to save useless writes, when a WALObject is already present in the WAL, it's replaced by the new version. That
 * means the element will not be written twice on disk.
 * @see <a href="https://en.wikipedia.org/wiki/Write-ahead_logging">https://en.wikipedia.org/wiki/Write-ahead_logging</a>
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface WALObject<K, V>
{
    /**
     * @return the B-tree information
     */
    public BTreeInfo<K, V> getBtreeInfo();

    
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
     * @param pageIOs Set the PageIOs for this object
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

    
    /**
     * Tells if the B-tree is a user B-tree or not
     * 
     * @return <code>false</code> if the B-tree is a <em>BtreeOfBtrees</em> or a <em>CopiedPagesBTree</em>, 
     * <code>true</code> otherwise
     */
    boolean isBTreeUser();
}
