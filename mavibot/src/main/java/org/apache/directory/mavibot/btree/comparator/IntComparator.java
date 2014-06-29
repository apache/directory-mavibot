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
 * Compares integers
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class IntComparator implements Comparator<Integer>
{
    /** A static instance of a IntComparator */
    public static final IntComparator INSTANCE = new IntComparator();

    /**
     * A private constructor of the IntComparator class
     */
    private IntComparator()
    {
    }


    /**
     * Compare two integers.
     *
     * @param integer1 First integer
     * @param integer2 Second integer
     * @return 1 if integer1 > integer2, 0 if integer1 == integer2, -1 if integer1 < integer2
     */
    public int compare( Integer integer1, Integer integer2 )
    {
        if ( integer1 == integer2 )
        {
            return 0;
        }

        if ( integer1 == null )
        {
            if ( integer2 == null )
            {
                return 0;
            }
            else
            {
                return -1;
            }
        }
        else
        {
            if ( integer2 == null )
            {
                return 1;
            }
            else
            {
                return integer1.compareTo( integer2 );
            }
        }
    }
}
