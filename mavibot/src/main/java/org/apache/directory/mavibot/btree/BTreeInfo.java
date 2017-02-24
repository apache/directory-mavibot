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
public class BTreeInfo<K, V> extends AbstractWALObject<K, V>
{
    /** the B-tree page nb elements */
    private int pageNbElem;
    
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
    
    /**
     * Serialize the structure :
     * 
     * <pre>
     * +------------+
     * | pageNbElem | The B-tree number of elements per page max
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
    public PageIO[] serialize( WriteTransaction transaction ) throws IOException
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
        
        RecordManager recordManager = transaction.getRecordManager();
        RecordManagerHeader recordManagerHeader = transaction.recordManagerHeader;


        // Get the pageIOs we need to store the data. We may need more than one.
        pageIOs = recordManager.getFreePageIOs( recordManagerHeader, bufferSize );

        // Now store the B-tree information data in the pages :
        // - the B-tree page size
        // - the B-tree name
        // - the keySerializer FQCN
        // - the valueSerializer FQCN
        // Starts at 0
        long position = 0L;
        
        // The B-tree page size
        position = recordManager.store( recordManagerHeader, position, btree.getPageNbElem(), pageIOs );

        // The tree name
        position = recordManager.store( recordManagerHeader, position, btreeNameBytes, pageIOs );

        // The keySerializer FQCN
        position = recordManager.store( recordManagerHeader, position, keySerializerBytes, pageIOs );

        // The valueSerialier FQCN
        recordManager.store( recordManagerHeader, position, valueSerializerBytes, pageIOs );
        
        // Set the BtreeInfo offset
        offset = pageIOs[0].getOffset();
        
        return pageIOs;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public BTreeInfo<K, V> deserialize( Transaction trasaction, ByteBuffer byteBuffer )
    {
        return null;
    }
    
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getName()
    {
        return btree.getName();
    }


    /**
     * @return the number of element that can be stored in this page
     */
    public int getPageNbElem()
    {
        return pageNbElem;
    }


    /**
     * @param pageNbElement the pageNbElem to set
     */
    public void setPageNbElem( int pageNbElem )
    {
        this.pageNbElem = pageNbElem;
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
     * {@inheritDoc}
     */
    @Override
    public long getRevision()
    {
        return -1L;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public String prettyPrint()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "{Info(" ).append( id ).append( ")@" );
        
        if ( offset == RecordManager.NO_PAGE )
        {
            sb.append( "---" );
        }
        else
        {
            sb.append( String.format( "0x%4X", offset ) );
        }
        
        sb.append( ",<" );
        sb.append( getName() ).append( ':' ).append( getRevision() );
        sb.append( ">}" );

        return sb.toString();
    }

    
    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "BtreeInfo :\n" );
        sb.append( "    Page Nb Elem     : " ).append( pageNbElem ).append( '\n' );
        sb.append( "    Name             : " ).append( btreeName ).append( '\n' );
        sb.append( "    Key serializer   : " ).append( keySerializerFQCN ).append( '\n' );
        sb.append( "    Value serializer : " ).append( valueSerializerFQCN ).append( '\n' );
        sb.append( "    ID               : " ).append( id ).append( '\n' );
        sb.append( "    offset           : " ).append( String.format( "%16x", getOffset() ) ).append( '\n' );

        return sb.toString();
    }
}
