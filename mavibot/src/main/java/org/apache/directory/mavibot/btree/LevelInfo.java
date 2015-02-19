/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 *
 */

package org.apache.directory.mavibot.btree;


/**
 * A class to store informations on a level. We have to keep :
 * <ul>
 * <li>The number of elements to store in this level</li>
 * <li>A flag that tells if it's a leaf or a node level</li>
 * <li>The number of pages necessary to store all the elements in a level</li>
 * <li>The number of elements we can store in a complete page (we may have one or two 
 * incomplete pages at the end)</li>
 * <li>A flag that tells if we have some incomplete page at the end</li>
 * </ul>
 *
 * @author <a href="mailto:dev@directory.apache.org">Apache Directory Project</a>
 */
public class LevelInfo<K, V>
{
    /** The level number */
    private int levelNumber;

    /** Nb of elements for this level */
    private int nbElems;

    /** The number of pages in this level */
    private int nbPages;

    /** Nb of elements before we reach an incomplete page */
    private int nbElemsLimit;

    /** A flag that tells if the level contains nodes or leaves */
    private boolean isNode;

    /** The current page which contains the data until we move it to the resulting BTree */
    private Page<K, V> currentPage;

    /** The current position in the currentPage */
    private int currentPos;

    /** The number of already added elements for this level */
    private int nbAddedElems;


    /**
     * @return the levelNumber
     */
    public int getLevelNumber()
    {
        return levelNumber;
    }


    /**
     * @param levelNumber the levelNumber to set
     */
    public void setLevelNumber( int levelNumber )
    {
        this.levelNumber = levelNumber;
    }


    /**
     * @return the nbElems
     */
    public int getNbElems()
    {
        return nbElems;
    }


    /**
     * @param nbElems the nbElems to set
     */
    public void setNbElems( int nbElems )
    {
        this.nbElems = nbElems;
    }


    /**
     * @return the nbPages
     */
    public int getNbPages()
    {
        return nbPages;
    }


    /**
     * @param nbPages the nbPages to set
     */
    public void setNbPages( int nbPages )
    {
        this.nbPages = nbPages;
    }


    /**
     * Increment the number of pages
     */
    public void incNbPages()
    {
        this.nbPages++;
    }


    /**
     * @return the nbElemsLimit
     */
    public int getNbElemsLimit()
    {
        return nbElemsLimit;
    }


    /**
     * @param nbElemsLimit the nbElemsLimit to set
     */
    public void setNbElemsLimit( int nbElemsLimit )
    {
        this.nbElemsLimit = nbElemsLimit;
    }


    /**
     * @return the isNode
     */
    public boolean isNode()
    {
        return isNode;
    }


    /**
     * @param isNode the isNode to set
     */
    public void setType( boolean isNode )
    {
        this.isNode = isNode;
    }


    /**
     * @return the currentPage
     */
    public Page<K, V> getCurrentPage()
    {
        return currentPage;
    }


    /**
     * @param currentPage the currentPage to set
     */
    public void setCurrentPage( Page<K, V> currentPage )
    {
        this.currentPage = currentPage;
    }


    /**
     * @return the currentPos
     */
    public int getCurrentPos()
    {
        return currentPos;
    }


    /**
     * @param currentPos the currentPos to set
     */
    public void setCurrentPos( int currentPos )
    {
        this.currentPos = currentPos;
    }


    /**
     * Increment the current position
     */
    public void incCurrentPos()
    {
        this.currentPos++;
    }


    /**
     * @return the nbAddedElems
     */
    public int getNbAddedElems()
    {
        return nbAddedElems;
    }


    /**
     * @param nbAddedElems the nbAddedElems to set
     */
    public void setNbAddedElems( int nbAddedElems )
    {
        this.nbAddedElems = nbAddedElems;
    }


    /**
     * Increment the number of added elements
     */
    public void incNbAddedElems()
    {
        this.nbAddedElems++;
    }


    /** @see Object#toString() */
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        if ( isNode )
        {
            sb.append( "NodeLevel[" );
            sb.append( levelNumber );
            sb.append( "] :" );
        }
        else
        {
            sb.append( "LeafLevel:" );
        }

        sb.append( "\n    nbElems           = " ).append( nbElems );
        sb.append( "\n    nbPages           = " ).append( nbPages );
        sb.append( "\n    nbElemsLimit      = " ).append( nbElemsLimit );
        sb.append( "\n    nbAddedElems      = " ).append( nbAddedElems );
        sb.append( "\n    currentPos        = " ).append( currentPos );
        sb.append( "\n    currentPage" );
        sb.append( "\n        nbKeys : " ).append( currentPage.getNbElems() );

        return sb.toString();
    }
}
