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
 * A serializer for the RevisionName object. The RevisionName will be serialized
 * as a long (the revision), followed by the String.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* no qualifier*/class RevisionNameSerializer extends AbstractElementSerializer<RevisionName>
{
    /** A static instance of a RevisionNameSerializer */
    /*No qualifier*/ final static RevisionNameSerializer INSTANCE = new RevisionNameSerializer();

    /**
     * Create a new instance of a RevisionNameSerializer
     */
    private RevisionNameSerializer()
    {
        super( RevisionNameComparator.INSTANCE );
    }


    /**
     * A static method used to deserialize a RevisionName from a byte array.
     *
     * @param in The byte array containing the RevisionName
     * @return A RevisionName instance
     */
    /* no qualifier*/static RevisionName deserialize( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a RevisionName from a byte array.
     *
     * @param in The byte array containing the RevisionName
     * @param start the position in the byte[] we will deserialize the RevisionName from
     * @return A RevisionName instance
     */
    /* no qualifier*/static RevisionName deserialize( byte[] in, int start )
    {
        // The buffer must be 8 bytes plus 4 bytes long (the revision is a long, and the name is a String
        if ( ( in == null ) || ( in.length < 12 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a RevisionName from a buffer with not enough bytes" );
        }

        long revision = LongSerializer.deserialize( in, start );
        String name = StringSerializer.deserialize( in, 8 + start );

        RevisionName revisionName = new RevisionName( revision, name );

        return revisionName;
    }


    /**
     * A static method used to deserialize a RevisionName from a byte array.
     *
     * @param in The byte array containing the RevisionName
     * @return A RevisionName instance
     */
    public RevisionName fromBytes( byte[] in )
    {
        return deserialize( in, 0 );
    }


    /**
     * A static method used to deserialize a RevisionName from a byte array.
     *
     * @param in The byte array containing the RevisionName
     * @param start the position in the byte[] we will deserialize the RevisionName from
     * @return A RevisionName instance
     */
    public RevisionName fromBytes( byte[] in, int start )
    {
        // The buffer must be 8 bytes plus 4 bytes long (the revision is a long, and the name is a String
        if ( ( in == null ) || ( in.length < 12 + start ) )
        {
            throw new SerializerCreationException( "Cannot extract a RevisionName from a buffer with not enough bytes" );
        }

        long revision = LongSerializer.deserialize( in, start );
        String name = StringSerializer.deserialize( in, 8 + start );

        RevisionName revisionName = new RevisionName( revision, name );

        return revisionName;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] serialize( RevisionName revisionName )
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
     * Serialize a RevisionName
     *
     * @param buffer the Buffer that will contain the serialized value
     * @param start the position in the buffer we will store the serialized RevisionName
     * @param value the value to serialize
     * @return The byte[] containing the serialized RevisionName
     */
    /* no qualifier*/static byte[] serialize( byte[] buffer, int start, RevisionName revisionName )
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
    public RevisionName deserialize( BufferHandler bufferHandler ) throws IOException
    {
        byte[] revisionBytes = bufferHandler.read( 8 );
        long revision = LongSerializer.deserialize( revisionBytes );

        byte[] lengthBytes = bufferHandler.read( 4 );

        int len = IntSerializer.deserialize( lengthBytes );

        switch ( len )
        {
            case 0:
                return new RevisionName( revision, "" );

            case -1:
                return new RevisionName( revision, null );

            default:
                byte[] nameBytes = bufferHandler.read( len );

                return new RevisionName( revision, Strings.utf8ToString( nameBytes ) );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public RevisionName deserialize( ByteBuffer buffer ) throws IOException
    {
        // The revision
        long revision = buffer.getLong();

        // The name's length
        int len = buffer.getInt();

        switch ( len )
        {
            case 0:
                return new RevisionName( revision, "" );

            case -1:
                return new RevisionName( revision, null );

            default:
                byte[] nameBytes = new byte[len];
                buffer.get( nameBytes );

                return new RevisionName( revision, Strings.utf8ToString( nameBytes ) );
        }
    }
}
