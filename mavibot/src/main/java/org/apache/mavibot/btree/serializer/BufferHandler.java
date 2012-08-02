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
package org.apache.mavibot.btree.serializer;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * A class used to hide the buffer read from the underlying file.
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BufferHandler
{
    /** The channel we read bytes from */
    private FileChannel channel;

    /** The buffer containing the bytes we read from the channel */
    private ByteBuffer buffer;


    /**
     * Create a new BufferHandler 
     * @param buffer The buffer used to transfer data
     */
    public BufferHandler( byte[] buffer )
    {
        this.buffer = ByteBuffer.allocate( buffer.length );
        this.buffer.put( buffer );
        this.buffer.flip();
    }


    /**
     * Create a new BufferHandler 
     * @param channel The channel to read
     * @param buffer The buffer used to transfer data
     */
    public BufferHandler( FileChannel channel, ByteBuffer buffer )
    {
        this.channel = channel;
        this.buffer = buffer;

        try
        {
            // Initial read
            channel.read( buffer );
            buffer.flip();
        }
        catch ( IOException ioe )
        {

        }
    }


    /**
     * Read a buffer containing the given number of bytes
     * @param len The number of bytes to read
     * @return
     */
    public byte[] read( int len ) throws IOException
    {
        byte[] result = new byte[len];

        if ( len <= buffer.remaining() )
        {
            buffer.get( result );

            return result;
        }

        int requested = len;
        int position = 0;

        while ( requested != 0 )
        {
            int nbRead = buffer.limit() - buffer.position();
            System.arraycopy( buffer.array(), buffer.position(), result, position, nbRead );

            buffer.clear();

            if ( channel != null )
            {
                channel.read( buffer );
                buffer.flip();
            }
            else
            {
                throw new IOException( "Not enough bytes in the buffer" );
            }

            requested -= nbRead;
        }

        return result;
    }
}
