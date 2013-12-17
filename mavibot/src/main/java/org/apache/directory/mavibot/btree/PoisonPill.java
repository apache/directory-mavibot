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
package org.apache.directory.mavibot.btree;


/**
 * This is special class which is injected into the journal queue to tell
 * the journal thread that it should stop.
 *
 * @param <K> The key type
 * @param <V> The value type
 * 
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
/* No qualifier*/class PoisonPill<K, V> extends Modification<K, V>
{
    /**
     * Create a new PoisonPill instance.
     * 
     * @param key The key being added
     * @param value The value being added
     */
    /* no qualifier */PoisonPill()
    {
        super( null, null );
    }
}
