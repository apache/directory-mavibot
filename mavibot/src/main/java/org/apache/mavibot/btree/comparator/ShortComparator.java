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
 * Compares shorts
 * 
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public class ShortComparator implements Comparator<Short>
{
    /**
     * Compare two shorts.
     * 
     * @param short1 First short
     * @param short2 Second short
     * @return 1 if short1 > short2, 0 if short1 == short2, -1 if short1 < short2
     */
    public int compare( Short short1, Short short2 )
    {
        if ( short1 == short2 )
        {
            return 0;
        }

        if ( short1 == null )
        {
            throw new IllegalArgumentException( "The first object to compare must not be null" );
        }

        if ( short2 == null )
        {
            throw new IllegalArgumentException( "The second object to compare must not be null" );
        }

        return short1.compareTo( short2 );
    }
}
