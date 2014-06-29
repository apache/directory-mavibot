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
package org.apache.directory.mavibot.btree.comparator;


import java.util.Comparator;


/**
 * Compares bytes
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ByteComparator implements Comparator<Byte>
{
    /** A static instance of a ByteComparator */
    public static final ByteComparator INSTANCE = new ByteComparator();

    /**
     * A private constructor of the ByteComparator class
     */
    private ByteComparator()
    {
    }


    /**
     * Compare two bytes.
     *
     * @param byte1 First byte
     * @param byte2 Second byte
     * @return 1 if byte1 > byte2, 0 if byte1 == byte2, -1 if byte1 < byte2
     */
    public int compare( Byte byte1, Byte byte2 )
    {
        if ( byte1 == byte2 )
        {
            return 0;
        }

        if ( byte1 == null )
        {
            return -1;
        }

        if ( byte2 == null )
        {
            return 1;
        }

        if ( byte1 < byte2 )
        {
            return -1;
        }
        else if ( byte1 > byte2 )
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }
}
