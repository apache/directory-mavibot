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

import org.apache.directory.mavibot.btree.exception.SerializerCreationException;
import org.apache.directory.mavibot.btree.serializer.AbstractElementSerializer;
import org.apache.directory.mavibot.btree.serializer.BufferHandler;
import org.apache.directory.mavibot.btree.serializer.LongArraySerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;


/**
 * A serializer for the RevisionOffset object. The RevisionOffset will be serialized
 * as a long (the revision), followed by the long[].
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class RevisionOffsetSerializer extends AbstractElementSerializer<RevisionOffset>
{
    /** A static instance of a RevisionOffsetSerializer */
    /*No qualifier*/ final static RevisionOffsetSerializer INSTANCE = new RevisionOffsetSerializer();

    /**
     * Create a new instance of a RevisionOffsetSerializer
     */
    private RevisionOffsetSerializer()
    {
        super( RevisionOffsetComparator.INSTANCE );
    }


    /**
     * A static method used to deserialize a RevisionOffset from a byte array.
     *
     * @param in The byte array containing the RevisionOffset
     * @return A RevisionOffset instance
     */
    /* no qualifier*/static RevisionOffset deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a RevisionOffset from a byte array.
     *
     * @param in The byte array containing the RevisionOffset
     * @param start the position in the byte[] we will deserialize the RevisionOffset from
     * @return A RevisionOffset instance
     */
    /* no qualifier*/static RevisionOffset deserialize( byte[] in, int start )
    {
        // The buffer must be 8 bytes
        if ( ( in == null ) || ( in.length < 8 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a RevisionOffset from a buffer with not enough bytes" );
        }

        long revision = LongSerializer.deserialize( in, start );
        
        try
        {
            long[] offsets = LongArraySerializer.INSTANCE.fromBytes( in, 8 + start );

            RevisionOffset RevisionOffset = new RevisionOffset( revision, offsets );
            
            return RevisionOffset;
        }
        catch( IOException e )
        {
            throw new RuntimeException( e );
        }
    }


    /**
     * A static method used to deserialize a RevisionOffset from a byte array.
     *
     * @param in The byte array containing the RevisionOffset
     * @return A RevisionOffset instance
     */
    public RevisionOffset fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a RevisionOffset from a byte array.
     *
     * @param in The byte array containing the RevisionOffset
     * @param start the position in the byte[] we will deserialize the RevisionOffset from
     * @return A RevisionOffset instance
     */
    public RevisionOffset fromBytes( byte[] in, int start )
    {
        // The buffer must be 8 bytes long (the revision is a long, and the name is a String
        if ( ( in == null ) || ( in.length < 8 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a RevisionOffset from a buffer with not enough bytes" );
        }

        return deserialize( in, start );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize( RevisionOffset RevisionOffset )
    {
        if ( RevisionOffset == null )
        {
            throw new SerializerCreationException( "The RevisionOffset instance should not be null " );
        }

        byte[] result = null;

        byte[] offsets = LongArraySerializer.INSTANCE.serialize( RevisionOffset.getOffsets() );
        result = new byte[8 + offsets.length];
        LongSerializer.serialize( result, 0, RevisionOffset.getRevision() );

        System.arraycopy( offsets, 0, result, 8, offsets.length );
        
        return result;
    }


    /**
     * Serialize a RevisionOffset
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized RevisionOffset
     * @param value the value to serialize
     * @return The byte[] containing the serialized RevisionOffset
     */
    /* no qualifier*/static byte[] serialize( byte[] buffer, int start, RevisionOffset RevisionOffset )
    {
        LongSerializer.serialize( buffer, start, RevisionOffset.getRevision() );
        
        byte[] offsets = LongArraySerializer.INSTANCE.serialize( RevisionOffset.getOffsets() );

        System.arraycopy( offsets, 0, buffer, 8 + start, offsets.length );
        
        return buffer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RevisionOffset deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] revisionBytes = bufferHandler.read( 8 );
        long revision = LongSerializer.deserialize( revisionBytes );

        long[] offsets = LongArraySerializer.INSTANCE.deserialize( bufferHandler );

        return new RevisionOffset( revision, offsets );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RevisionOffset deserialize( ByteBuffer buffer ) throws IOException
    {
        // The revision
        long revision = buffer.getLong();

        long[] offsets = LongArraySerializer.INSTANCE.deserialize( buffer );

        return new RevisionOffset( revision, offsets );
    }
}
