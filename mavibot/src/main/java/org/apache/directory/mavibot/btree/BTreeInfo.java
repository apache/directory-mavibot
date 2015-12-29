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

import org.apache.directory.mavibot.btree.serializer.ElementSerializer;
import org.apache.directory.mavibot.btree.util.Strings;

/**
 * This class stores the informations contained in the BTree info header :
 * - the B-tree page size
 * - the B-tree name
 * - the keySerializer FQCN
 * - the valueSerializer FQCN
 * - the flags that tell if the dups are allowed
 *  
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BTreeInfo<K, V> implements WALObject
{
    /** The BTree reference */
    private BTree<K,V> btree;
    
    /** the B-tree page size */
    private int pageSize;
    
    /** the B-tree name */
    private String btreeName;
    
    /** the keySerializer FQCN */
    private String keySerializerFQCN;
    
    /** The key serializer instance */
    private ElementSerializer<K> keySerializer;

    /** the valueSerializer FQCN */
    private String valueSerializerFQCN;
    
    /** The value serializer instance */
    private ElementSerializer<V> valueSerializer;
    
    /** The unique ID for this object */
    private long id;
    
    /**
     * Serialize the structure :
     * 
     * <pre>
     * +------------+
     * | pageSize   | The B-tree page size (ie, the number of elements per page max)
     * +------------+
     * | nameSize   | The B-tree name size
     * +------------+
     * | name       | The B-tree name
     * +------------+
     * | keySerSize | The keySerializer FQCN size
     * +------------+
     * | keySerFQCN | The keySerializer FQCN
     * +------------+
     * | valSerSize | The Value serializer FQCN size
     * +------------+
     * | valSerKQCN | The valueSerializer FQCN
     * +------------+
     * </pre>
     * 
     * {@inheritDoc}
     * @throws IOException 
     */
    @Override
    public PageIO[] serialize() throws IOException
    {
        // We will add the newly managed B-tree at the end of the header.
        byte[] btreeNameBytes = Strings.getBytesUtf8( btree.getName() );
        byte[] keySerializerBytes = Strings.getBytesUtf8( btree.getKeySerializerFQCN() );
        byte[] valueSerializerBytes = Strings.getBytesUtf8( btree.getValueSerializerFQCN() );

        int bufferSize =
            RecordManager.INT_SIZE + // The page size
                RecordManager.INT_SIZE + // The name size
                btreeNameBytes.length + // The name
                RecordManager.INT_SIZE + // The keySerializerBytes size
                keySerializerBytes.length + // The keySerializerBytes
                RecordManager.INT_SIZE + // The valueSerializerBytes size
                valueSerializerBytes.length; // The valueSerializerBytes
        
        RecordManager recordManager = btree.getRecordManager();

        // Get the pageIOs we need to store the data. We may need more than one.
        PageIO[] btreeHeaderPageIos = recordManager.getFreePageIOs( bufferSize );

        // Now store the B-tree information data in the pages :
        // - the B-tree page size
        // - the B-tree name
        // - the keySerializer FQCN
        // - the valueSerializer FQCN
        // Starts at 0
        long position = 0L;

        // The B-tree page size
        position = recordManager.store( position, btree.getPageSize(), btreeHeaderPageIos );

        // The tree name
        position = recordManager.store( position, btreeNameBytes, btreeHeaderPageIos );

        // The keySerializer FQCN
        position = recordManager.store( position, keySerializerBytes, btreeHeaderPageIos );

        // The valueSerialier FQCN
        position = recordManager.store( position, valueSerializerBytes, btreeHeaderPageIos );

        // And flush the pages to disk now
        recordManager.storePages( btreeHeaderPageIos );

        return btreeHeaderPageIos;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getId()
    {
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setId( long id )
    {
        this.id = id;
    }


    /**
     * @return the btree
     */
    public BTree<K, V> getBtree()
    {
        return btree;
    }


    /**
     * @param btree the btree to set
     */
    public void setBtree( BTree<K, V> btree )
    {
        this.btree = btree;
    }


    /**
     * @return the pageSize
     */
    public int getPageSize()
    {
        return pageSize;
    }


    /**
     * @param pageSize the pageSize to set
     */
    public void setPageSize( int pageSize )
    {
        this.pageSize = pageSize;
    }


    /**
     * @return the btreeName
     */
    public String getBtreeName()
    {
        return btreeName;
    }


    /**
     * @param btreeName the btreeName to set
     */
    public void setBtreeName( String btreeName )
    {
        this.btreeName = btreeName;
    }


    /**
     * @return the keySerializer FQCN
     */
    public String getKeySerializerFQCN()
    {
        return keySerializerFQCN;
    }


    /**
     * @param keySerializerFQCN the keySerializer FQCN to set
     */
    public void setKeySerializerFQCN( String keySerializerFQCN )
    {
        this.keySerializerFQCN = keySerializerFQCN;
    }
    
    
    /**
     * @return the keySerializer
     */
    public ElementSerializer<K> getKeySerializer()
    {
        return keySerializer;
    }


    /**
     * @param keySerializer the keySerializer to set
     */
    public void setKeySerializer( ElementSerializer<K> keySerializer )
    {
        this.keySerializer = keySerializer;
    }


    /**
     * @return the valueSerializer FQCN
     */
    public String getValueSerializerFQCN()
    {
        return valueSerializerFQCN;
    }


    /**
     * @param valueSerializerFQCN the valueSerializer FQCN to set
     */
    public void setValueSerializerFQCN( String valueSerializerFQCN )
    {
        this.valueSerializerFQCN = valueSerializerFQCN;
    }


    /**
     * @return the valueSerializer
     */
    public ElementSerializer<V> getValueSerializer()
    {
        return valueSerializer;
    }


    /**
     * @param valueSerializer the valueSerializer to set
     */
    public void setValueSerializer( ElementSerializer<V> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
    }
    
    
    /**
     * @see Object#toString()
     */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "BtreeInfo :\n" );
        sb.append( "    Page size        : " ).append( pageSize ).append( '\n' );
        sb.append( "    Name             : " ).append( btreeName ).append( '\n' );
        sb.append( "    Key serializer   : " ).append( keySerializerFQCN ).append( '\n' );
        sb.append( "    Value serializer : " ).append( valueSerializerFQCN ).append( '\n' );

        return sb.toString();
    }
}
