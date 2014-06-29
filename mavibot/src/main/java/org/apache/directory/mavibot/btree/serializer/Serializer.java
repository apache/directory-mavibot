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


/**
 * This interface is used by implementations of serializer, deserializr and comparator.
 * 
 * @param <T> The type for the element to serialize
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public interface Serializer<T>
{
    /**
     * Produce the byte[] representation of the type
     * 
     * @param type The type to serialize
     * @return The byte[] containing the serialized type
     */
    byte[] serialize( T type );


    /**
     * Deserialize a type from a byte[]
     * 
     * @param bufferHandler The incoming BufferHandler
     * @return The deserialized type
     * @throws IOException If the deserialization failed
     */
    T deserialize( BufferHandler bufferHandler ) throws IOException;


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
}
