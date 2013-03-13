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
package org.apache.mavibot.btree;


/**
 * An enum to describe the BTree type. We have three possible type :
 * <ul>
 * <li>IN_MEMORY : the BTree will remain in memory, and won't be persisted on disk</li>
 * <li>PERSISTENT : the BTree is in memory, but will be persisted on disk</li>
 * <li>MANAGED : the BTree is managed by a RecordManager, and some pages may
 * be swapped out from memory on demand</li>
 * </ul>
 * @author <a href="mailto:labs@labs.apache.org">Mavibot labs Project</a>
 */
public enum BTreeTypeEnum
{
    /** Pure in-memory BTree, not persisted on disk */
    IN_MEMORY,

    /** In-memory BTree but persisted on disk */
    PERSISTENT,

    /** A BTree associated with a RecordManager */
    MANAGED
}
