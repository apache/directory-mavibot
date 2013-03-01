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
 * Compares long arrays
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class LongArrayComparator implements Comparator<long[]>
{
    /**
     * Compare two long arrays.
     * 
     * @param longArray1 First longArray
     * @param longArray2 Second longArray
     * @return 1 if longArray1 > longArray2, 0 if longArray1 == longArray2, -1 if longArray1 < longArray2
     */
    public int compare( long[] longArray1, long[] longArray2 )
    {
        if ( longArray1 == longArray2 )
        {
            return 0;
        }

        if ( longArray1 == null )
        {
            throw new IllegalArgumentException( "The first object to compare must not be null" );
        }

        if ( longArray2 == null )
        {
            throw new IllegalArgumentException( "The second object to compare must not be null" );
        }

        if ( longArray1.length < longArray2.length )
        {
            return -1;
        }

        if ( longArray1.length > longArray2.length )
        {
            return 1;
        }

        for ( int pos = 0; pos < longArray1.length; pos++ )
        {
            int comp = compare( longArray1[pos], longArray2[pos] );

            if ( comp != 0 )
            {
                return comp;
            }
        }

        return 0;
    }


    private int compare( long long1, long long2 )
    {
        if ( long1 < long2 )
        {
            return -1;
        }
        if ( long1 > long2 )
        {
            return 1;
        }

        return 0;
    }
}
