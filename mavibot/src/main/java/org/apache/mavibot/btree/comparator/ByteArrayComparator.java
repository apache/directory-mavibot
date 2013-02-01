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
package org.apache.mavibot.btree.comparator;


import java.util.Comparator;


/**
 * Compares byte arrays
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class ByteArrayComparator implements Comparator<byte[]>
{
    /**
     * Compare two byte arrays.
     * 
     * @param byteArray1 First byteArray
     * @param byteArray2 Second byteArray
     * @return 1 if byteArray1 > byteArray2, 0 if byteArray1 == byteArray2, -1 if byteArray1 < byteArray2
     */
    public int compare( byte[] byteArray1, byte[] byteArray2 )
    {
        if ( byteArray1 == byteArray2 )
        {
            return 0;
        }

        if ( byteArray1 == null )
        {
            throw new IllegalArgumentException( "The first object to compare must not be null" );
        }

        if ( byteArray2 == null )
        {
            throw new IllegalArgumentException( "The second object to compare must not be null" );
        }

        if ( byteArray1.length < byteArray2.length )
        {
            return -1;
        }

        if ( byteArray1.length > byteArray2.length )
        {
            return 1;
        }

        for ( int pos = 0; pos < byteArray1.length; pos++ )
        {
            int comp = compare( byteArray1[pos], byteArray2[pos] );

            if ( comp != 0 )
            {
                return comp;
            }
        }

        return 0;
    }


    private int compare( byte byte1, byte byte2 )
    {
        if ( byte1 < byte2 )
        {
            return -1;
        }
        if ( byte1 > byte2 )
        {
            return 1;
        }

        return 0;
    }
}
