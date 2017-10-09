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
    private String name;
    
    /** the keySerializer FQCN */
    private String keySerializerFQCN;
    
    /** The key serializer instance */
    private ElementSerializer<K> keySerializer;

    /** the valueSerializer FQCN */
    private String valueSerializerFQCN;
    
    /** The value serializer instance */
    private ElementSerializer<V> valueSerializer;
    
    /** The BTree type : either in-memory, disk backed or persisted */
    private BTreeTypeEnum btreeType;

    
    /**
     * Default constructor
     */
    public BTreeInfo()
    {
        btreeInfo = this;
    }
    
    /**
     * Serialize the BTreeInfo structure :
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
        byte[] btreeNameBytes = Strings.getBytesUtf8( name );
        byte[] keySerializerBytes = Strings.getBytesUtf8( keySerializerFQCN );
        byte[] valueSerializerBytes = Strings.getBytesUtf8( valueSerializerFQCN );

        int bufferSize =
            BTreeConstants.INT_SIZE + // The page size
                BTreeConstants.INT_SIZE + // The name size
                btreeNameBytes.length + // The name
                BTreeConstants.INT_SIZE + // The keySerializerBytes size
                keySerializerBytes.length + // The keySerializerBytes
                BTreeConstants.INT_SIZE + // The valueSerializerBytes size
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
        position = recordManager.store( recordManagerHeader, position, pageNbElem, pageIOs );

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
    public String getName()
    {
        return name;
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
     * @param btreeName the btreeName to set
     */
    public void setName( String name )
    {
        this.name = name;
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
        
        try
        {
            Class<?> instance = Class.forName( keySerializerFQCN );
        
            keySerializer = ( ElementSerializer<K> ) instance.getDeclaredField( "INSTANCE" ).get( null );

            if ( keySerializer == null )
            {
                keySerializer = ( ElementSerializer<K> ) instance.newInstance();
            }
        }
        catch ( ClassNotFoundException | NoSuchFieldException | IllegalAccessException | InstantiationException e )
        {
            // Add log
        }
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
        
        try
        {
            Class<?> instance = Class.forName( valueSerializerFQCN );
        
            valueSerializer = ( ElementSerializer<V> ) instance.getDeclaredField( "INSTANCE" ).get( null );

            if ( valueSerializer == null )
            {
                valueSerializer = ( ElementSerializer<V> ) instance.newInstance();
            }
        }
        catch ( ClassNotFoundException | NoSuchFieldException | IllegalAccessException | InstantiationException e )
        {
            // Add log
        }
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
     * 
     * The BTreeInfo revision is always -1L
     */
    @Override
    public long getRevision()
    {
        return -1L;
    }


    /**
     * @return the type
     */
    public BTreeTypeEnum getType()
    {
        return btreeType;
    }


    /**
     * @param type the type to set
     */
    public void setType( BTreeTypeEnum type )
    {
        this.btreeType = type;
    }

    
    /**
     * {@inheritDoc}
     */
    @Override
    public String prettyPrint()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "{Info(" ).append( id ).append( ")@" );
        
        if ( offset == BTreeConstants.NO_PAGE )
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
        sb.append( "    Name             : " ).append( name ).append( '\n' );
        sb.append( "    Type             : " ).append( btreeType ).append( '\n' );
        sb.append( "    Key serializer   : " ).append( keySerializerFQCN ).append( '\n' );
        sb.append( "    Value serializer : " ).append( valueSerializerFQCN ).append( '\n' );
        sb.append( "    ID               : " ).append( id ).append( '\n' );
        sb.append( "    offset           : " ).append( String.format( "%16x", getOffset() ) ).append( '\n' );

        return sb.toString();
    }
}
