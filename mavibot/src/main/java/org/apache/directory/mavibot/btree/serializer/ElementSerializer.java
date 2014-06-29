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
package org.apache.directory.mavibot.btree.serializer;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;


/**
 * This interface is used by implementations of serializer, deserializer and comparator.
 * 
 * @param <T> The type for the element to serialize and compare
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface ElementSerializer<T>
{
    /**
     * Produce the byte[] representation of the element
     * 
     * @param key The element to serialize
     * @return The byte[] containing the serialized element
     */
    byte[] serialize( T key );


    /**
     * Deserialize an element from a BufferHandler
     * 
     * @param bufferHandler The incoming bufferHandler
     * @return The deserialized element
     * @throws IOException If the deserialization failed
     */
    T deserialize( BufferHandler bufferHandler ) throws IOException;


    /**
     * Deserialize an element from a ByteBuffer
     * 
     * @param buffer The incoming ByteBuffer
     * @return The deserialized element
     * @throws IOException If the deserialization failed
     */
    T deserialize( ByteBuffer buffer ) throws IOException;


    /**
     * Deserialize an element from a byte[]
     * 
     * @param buffer The incoming byte[]
     * @return The deserialized element
     * @throws IOException If the deserialization failed
     */
    T fromBytes( byte[] buffer ) throws IOException;


    /**
     * Deserialize an element from a byte[]
     * 
     * @param buffer The incoming byte[]
     * @return The deserialized element
     * @throws IOException If the deserialization failed
     */
    T fromBytes( byte[] buffer, int pos ) throws IOException;


    /**
     * Returns the comparison of two types. <br/>
     * <ul>
     * <li>If type1 < type2, return -1</li>
     * <li>If type1 > type2, return 1</li>
     * <li>If type1 == type2, return 0</li>
     * </ul>
     * 
     * @param type1 The first type to compare 
     * @param type2 The second type to compare 
     * @return The comparison result
     */
    int compare( T type1, T type2 );


    /**
     * @return the comparator for the used type
     */
    Comparator<T> getComparator();


    /**
     * @return the type being serialized
     */
    Class<?> getType();
}
