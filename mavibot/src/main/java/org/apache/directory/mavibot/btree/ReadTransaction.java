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


import java.io.Closeable;
import java.io.IOException;
import java.util.Date;


/**
 * A read transaction is used to insure that reads are always done against 
 * one single revision.
 * <br>
 * A read transaction should be closed when the user is done with it, otherwise the
 * pages associated with the given revision, and all the referenced pages, will
 * remain on the storage. 
 * <p/>
 * A Transaction can be hold for quite a long time, for instance while doing
 * a browse against a big b-tree. At some point, transactions which are pending
 * for too long will be closed by the transaction manager (the default is to keep a 
 * read transaction opened for 30 s). If one need to keep a read transaction opened
 * for a longer time, pass a longer timeout to the constructor. 
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class ReadTransaction extends AbstractTransaction implements Closeable
{
    /**
     * Creates a new read transaction, with a default 30 seconds timeout.
     *
     * @param recordManager The associated RecordManager
     */
    /* No qualifier*/ ReadTransaction( RecordManager recordManager )
    {
        super( recordManager );
        
        // Don't get a copy of the RMH
        recordManagerHeader = recordManager.getCurrentRecordManagerHeader();
    }
    
    /**
     * Creates a new read transaction, with a specific tiemout.
     *
     * @param recordManager The associated RecordManager
     * @param timeout The transaction's timeout
     */
    /* No qualifier*/ ReadTransaction( RecordManager recordManager, long timeout )
    {
        super( recordManager, timeout );
        
        // Don't get a copy of the RMH
        recordManagerHeader = recordManager.getCurrentRecordManagerHeader();
    }


    /**
     * {@inheritDoc}
     * @throws IOException
     */
    @Override
    public void close() throws IOException
    {
        commit();
    }


    /**
     * @see Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append( "ReadTransaction[" );
        sb.append( getRevision() );
        sb.append( ", " );
        sb.append( new Date( getCreationDate() ) );
        sb.append( ", " );
        sb.append( getTimeout() );
        
        if ( isClosed() )
        {
            sb.append( ", CLOSED" );
        }
        
        sb.append( "]" );
        
        return sb.toString();
    }
}
