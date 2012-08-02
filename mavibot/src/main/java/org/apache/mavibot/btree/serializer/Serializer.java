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


/**
 * This interface is used by implementations of the key and value serializers.
 * 
 * @param <K> The type for the keys
 * @param <V> The type for the stored values
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public interface Serializer<K, V>
{
    /**
     * Produce the byte[] representation of the key
     * 
     * @param key The key to serialize
     * @return The byte[] containing the serialized key
     */
    byte[] serializeKey( K key );


    /**
     * Deserialize a key from a byte[]
     * 
     * @param bufferHandler The incoming BufferHandler
     * @return The deserialized key
     * @throws IOException If the deserialization failed
     */
    K deserializeKey( BufferHandler bufferHandler ) throws IOException;


    /**
     * Produce the byte[] representation of the value
     * 
     * @param value The value to serialize
     * @return The byte[] containing the serialized value
     */
    byte[] serializeValue( V value );


    /**
     * Deserialize a value from a byte[]
     * 
     * @param bufferHandler The incoming BufferHandler
     * @return The deserialized value
     * @throws IOException If the deserialization failed
     */
    V deserializeValue( BufferHandler bufferHandler ) throws IOException;
}
