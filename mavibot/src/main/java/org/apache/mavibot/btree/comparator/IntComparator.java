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
 * Compares integers
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class IntComparator implements Comparator<Integer>
{
    /**
     * Compare two integers.
     * 
     * @param integer1 First integer
     * @param integer2 Second integer
     * @return 1 if long1 > long2, 0 if long1 == long2, -1 if long1 < long2
     */
    public int compare( Integer integer1, Integer integer2 )
    {
        if ( integer1 == integer2 )
        {
            return 0;
        }

        if ( integer1 == null )
        {
            throw new IllegalArgumentException( "The first object to compare must not be null" );
        }

        if ( integer2 == null )
        {
            throw new IllegalArgumentException( "The second object to compare must not be null" );
        }

        return integer1.compareTo( integer2 );
    }
}
