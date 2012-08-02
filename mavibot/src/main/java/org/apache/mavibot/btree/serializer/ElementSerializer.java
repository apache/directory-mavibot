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
 * This interface is used by implementations elements serializers.
 * 
 * @param <T> The type for the element
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
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
     * Deserialize an element from a byte[]
     * 
     * @param in The incoming bufferHandler
     * @return The deserialized element
     * @throws IOException If the deserialization failed
     */
    T deserialize( BufferHandler bufferHandler ) throws IOException;
}
