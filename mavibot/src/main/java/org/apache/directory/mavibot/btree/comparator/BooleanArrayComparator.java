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
 * Compares boolean arrays. A boolean is considered as below the other one if the first boolean
 * is false when the second one is true.
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class BooleanArrayComparator implements Comparator<boolean[]>
{
    /** A static instance of a BooleanArrayComparator */
    public static final BooleanArrayComparator INSTANCE = new BooleanArrayComparator();

    /**
     * A private constructor of the BooleanArrayComparator class
     */
    private BooleanArrayComparator()
    {
    }


    /**
     * Compare two boolean arrays.
     *
     * @param booleanArray1 First boolean array
     * @param booleanArray2 Second boolean array
     * @return 1 if booleanArray1 > booleanArray2, 0 if booleanArray1 == booleanArray2, -1 if booleanArray1 < booleanArray2
     */
    public int compare( boolean[] booleanArray1, boolean[] booleanArray2 )
    {
        if ( booleanArray1 == booleanArray2 )
        {
            return 0;
        }

        if ( booleanArray1 == null )
        {
            return -1;
        }

        if ( booleanArray2 == null )
        {
            return 1;
        }

        if ( booleanArray1.length < booleanArray2.length )
        {
            return -1;
        }

        if ( booleanArray1.length > booleanArray2.length )
        {
            return 1;
        }

        for ( int pos = 0; pos < booleanArray1.length; pos++ )
        {
            int comp = compare( booleanArray1[pos], booleanArray2[pos] );

            if ( comp != 0 )
            {
                return comp;
            }
        }

        return 0;
    }


    private int compare( boolean boolean1, boolean boolean2 )
    {
        if ( boolean1 == boolean2 )
        {
            return 0;
        }

        if ( boolean1 )
        {
            return 1;
        }
        else
        {
            return -1;
        }
    }
}
