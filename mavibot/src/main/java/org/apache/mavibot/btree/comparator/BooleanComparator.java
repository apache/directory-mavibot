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
 * Compares booleans
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class BooleanComparator implements Comparator<Boolean>
{
    /**
     * Compare two booleans.
     * 
     * @param boolean1 First boolean
     * @param boolean2 Second boolean
     * @return 1 if boolean1 > boolean2, 0 if boolean1 == boolean2, -1 if boolean1 < boolean2
     */
    public int compare( Boolean boolean1, Boolean boolean2 )
    {
        if ( boolean1 == boolean2 )
        {
            return 0;
        }

        if ( boolean1 == null )
        {
            throw new IllegalArgumentException( "The first object to compare must not be null" );
        }

        if ( boolean2 == null )
        {
            throw new IllegalArgumentException( "The second object to compare must not be null" );
        }

        return boolean1.compareTo( boolean2 );
    }
}
