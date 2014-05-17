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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.directory.mavibot.btree.serializer.BufferHandler;
import org.apache.directory.mavibot.btree.serializer.LongSerializer;
import org.apache.directory.mavibot.btree.serializer.StringSerializer;
import org.junit.Test;


/**
 * Test the RevisionNameSerializer class
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class RevisionNameSerializerTest
{
    private static RevisionNameSerializer serializer = RevisionNameSerializer.INSTANCE;


    @Test
    public void testRevisionNameSerializer() throws IOException
    {
        RevisionName value = null;

        try
        {
            serializer.serialize( value );
            fail();
        }
        catch ( Exception e )
        {
            //exptected
        }

        // ------------------------------------------------------------------
        value = new RevisionName( 1L, null );
        byte[] result = serializer.serialize( value );

        assertEquals( 12, result.length );

        assertEquals( 1L, ( long ) LongSerializer.deserialize( result ) );
        assertNull( StringSerializer.deserialize( result, 8 ) );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );

        // ------------------------------------------------------------------
        value = new RevisionName( 0L, "" );
        result = serializer.serialize( value );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );

        // ------------------------------------------------------------------
        value = new RevisionName( 0L, "L\u00E9charny" );
        result = serializer.serialize( value );

        assertEquals( value, serializer.deserialize( new BufferHandler( result ) ) );
    }
}
