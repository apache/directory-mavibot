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
import org.apache.directory.mavibot.btree.serializer.ByteArraySerializer;
import org.apache.directory.mavibot.btree.serializer.IntSerializer;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.apache.directory.mavibot.btree.util.Strings;


/**
 * A serializer for the NameRevision object. The NameRevision will be serialized
 * as a String ( the Name) followed by the revision as a Long.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class NameRevisionSerializer extends AbstractElementSerializer<NameRevision>
{
    /** A static instance of a NameRevisionSerializer */
    /*No qualifier*/ final static NameRevisionSerializer INSTANCE = new NameRevisionSerializer();

    /**
     * Create a new instance of a NameRevisionSerializer
     */
    private NameRevisionSerializer()
    {
        super( NameRevisionComparator.INSTANCE );
    }


    /**
     * A static method used to deserialize a NameRevision from a byte array.
     *
     * @param in The byte array containing the NameRevision
     * @return A NameRevision instance
     */
    /* no qualifier*/static NameRevision deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a NameRevision from a byte array.
     *
     * @param in The byte array containing the NameRevision
     * @param start the position in the byte[] we will deserialize the NameRevision from
     * @return A NameRevision instance
     */
    /* no qualifier*/static NameRevision deserialize( byte[] in, int start )
    {
        // The buffer must be 8 bytes plus 4 bytes long (the revision is a long, and the name is a String
        if ( ( in == null ) || ( in.length < 12 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a NameRevision from a buffer with not enough bytes" );
        }

        long revision = LongSerializer.deserialize( in, start );
        String name = StringSerializer.deserialize( in, 8 + start );

        NameRevision revisionName = new NameRevision( name, revision );

        return revisionName;
    }


    /**
     * A static method used to deserialize a NameRevision from a byte array.
     *
     * @param in The byte array containing the NameRevision
     * @return A NameRevision instance
     */
    public NameRevision fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a NameRevision from a byte array.
     *
     * @param in The byte array containing the NameRevision
     * @param start the position in the byte[] we will deserialize the NameRevision from
     * @return A NameRevision instance
     */
    public NameRevision fromBytes( byte[] in, int start )
    {
        // The buffer must be 8 bytes plus 4 bytes long (the revision is a long, and the name is a String
        if ( ( in == null ) || ( in.length < 12 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a NameRevision from a buffer with not enough bytes" );
        }

        long revision = LongSerializer.deserialize( in, start );
        String name = StringSerializer.deserialize( in, 8 + start );

        NameRevision revisionName = new NameRevision( name, revision );

        return revisionName;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize( NameRevision revisionName )
    {
        if ( revisionName == null )
        {
            throw new SerializerCreationException( "The revisionName instance should not be null " );
        }

        byte[] result = null;

        if ( revisionName.getName() != null )
        {
            byte[] stringBytes = Strings.getBytesUtf8( revisionName.getName() );
            int stringLen = stringBytes.length;
            result = new byte[8 + 4 + stringBytes.length];
            LongSerializer.serialize( result, 0, revisionName.getRevision() );

            if ( stringLen > 0 )
            {
                ByteArraySerializer.serialize( result, 8, stringBytes );
            }
        }
        else
        {
            result = new byte[8 + 4];
            LongSerializer.serialize( result, 0, revisionName.getRevision() );
            StringSerializer.serialize( result, 8, null );
        }

        return result;
    }


    /**
     * Serialize a NameRevision
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized NameRevision
     * @param value the value to serialize
     * @return The byte[] containing the serialized NameRevision
     */
    /* no qualifier*/static byte[] serialize( byte[] buffer, int start, NameRevision revisionName )
    {
        if ( revisionName.getName() != null )
        {
            byte[] stringBytes = Strings.getBytesUtf8( revisionName.getName() );
            int stringLen = stringBytes.length;
            LongSerializer.serialize( buffer, start, revisionName.getRevision() );
            IntSerializer.serialize( buffer, 8 + start, stringLen );
            ByteArraySerializer.serialize( buffer, 12 + start, stringBytes );
        }
        else
        {
            LongSerializer.serialize( buffer, start, revisionName.getRevision() );
            StringSerializer.serialize( buffer, 8, null );
        }

        return buffer;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public NameRevision deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] revisionBytes = bufferHandler.read( 8 );
        long revision = LongSerializer.deserialize( revisionBytes );

        byte[] lengthBytes = bufferHandler.read( 4 );

        int len = IntSerializer.deserialize( lengthBytes );

        switch ( len )
        {
            case 0:
                return new NameRevision( "", revision );

            case -1:
                return new NameRevision( null, revision );

            default:
                byte[] nameBytes = bufferHandler.read( len );

                return new NameRevision( Strings.utf8ToString( nameBytes ), revision );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public NameRevision deserialize( ByteBuffer buffer ) throws IOException
    {
        // The revision
        long revision = buffer.getLong();

        // The name's length
        int len = buffer.getInt();

        switch ( len )
        {
            case 0:
                return new NameRevision( "", revision );

            case -1:
                return new NameRevision( null, revision );

            default:
                byte[] nameBytes = new byte[len];
                buffer.get( nameBytes );

                return new NameRevision( Strings.utf8ToString( nameBytes ), revision );
        }
    }
}
