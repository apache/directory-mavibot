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
 * A serializer for the RevisionAndHeaderOffset object. The RevisionAndHeaderOffset will be serialized
 * as a long (the revision), followed by the long[].
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class RevisionAndHeaderOffsetSerializer extends AbstractElementSerializer<RevisionAndHeaderOffset>
{
    /** A static instance of a RevisionAndHeaderOffsetSerializer */
    /*No qualifier*/ final static RevisionAndHeaderOffsetSerializer INSTANCE = new RevisionAndHeaderOffsetSerializer();

    /**
     * Create a new instance of a RevisionAndHeaderOffsetSerializer
     */
    private RevisionAndHeaderOffsetSerializer()
    {
        super( RevisionAndHeaderOffsetComparator.INSTANCE );
    }


    /**
     * A static method used to deserialize a RevisionAndHeaderOffset from a byte array.
     *
     * @param in The byte array containing the RevisionAndHeaderOffset
     * @return A RevisionAndHeaderOffset instance
     */
    /* no qualifier*/static RevisionAndHeaderOffset deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a RevisionAndHeaderOffset from a byte array.
     *
     * @param in The byte array containing the RevisionAndHeaderOffset
     * @param start the position in the byte[] we will deserialize the RevisionAndHeaderOffset from
     * @return A RevisionAndHeaderOffset instance
     */
    /* no qualifier*/static RevisionAndHeaderOffset deserialize( byte[] in, int start )
    {
        // The buffer must be 8 bytes
        if ( ( in == null ) || ( in.length < 8 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a RevisionAndHeaderOffset from a buffer with not enough bytes" );
        }

        long revision = LongSerializer.deserialize( in, start );
        
        long offset = LongSerializer.deserialize( in, start + 8 );
        
        RevisionAndHeaderOffset RevisionOffset = new RevisionAndHeaderOffset( revision, offset );
        
        return RevisionOffset;
    }


    /**
     * A static method used to deserialize a RevisionAndHeaderOffset from a byte array.
     *
     * @param in The byte array containing the RevisionAndHeaderOffset
     * @return A RevisionAndHeaderOffset instance
     */
    public RevisionAndHeaderOffset fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a RevisionAndHeaderOffset from a byte array.
     *
     * @param in The byte array containing the RevisionAndHeaderOffset
     * @param start the position in the byte[] we will deserialize the RevisionAndHeaderOffset from
     * @return A RevisionAndHeaderOffset instance
     */
    public RevisionAndHeaderOffset fromBytes( byte[] in, int start )
    {
        // The buffer must be 8 bytes long (the revision is a long, and the name is a String
        if ( ( in == null ) || ( in.length < 8 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a RevisionAndHeaderOffset from a buffer with not enough bytes" );
        }

        return deserialize( in, start );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize( RevisionAndHeaderOffset RevisionOffset )
    {
        if ( RevisionOffset == null )
        {
            throw new SerializerCreationException( "The RevisionAndHeaderOffset instance should not be null " );
        }

        byte[] result = new byte[16];
        LongSerializer.serialize( result, 0, RevisionOffset.getRevision() );

        LongSerializer.serialize( result, 8, RevisionOffset.getOffset() );
        
        return result;
    }


    /**
     * Serialize a RevisionAndHeaderOffset
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized RevisionAndHeaderOffset
     * @param value the value to serialize
     * @return The byte[] containing the serialized RevisionAndHeaderOffset
     */
    /* no qualifier*/static byte[] serialize( byte[] buffer, int start, RevisionAndHeaderOffset RevisionOffset )
    {
        LongSerializer.serialize( buffer, start, RevisionOffset.getRevision() );
        LongSerializer.serialize( buffer, start + 8, RevisionOffset.getOffset() );
        return buffer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RevisionAndHeaderOffset deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] revisionBytes = bufferHandler.read( 16 );
        long revision = LongSerializer.deserialize( revisionBytes );

        long offset = LongSerializer.deserialize( revisionBytes, 8 );

        return new RevisionAndHeaderOffset( revision, offset );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RevisionAndHeaderOffset deserialize( ByteBuffer buffer ) throws IOException
    {
        // The revision
        long revision = buffer.getLong();

        long offset = buffer.getLong();

        return new RevisionAndHeaderOffset( revision, offset );
    }
}
