/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.journal.impl;

import org.jboss.messaging.core.journal.SequentialFile;

/**
 * 
 * A JournalFile
 * 
 * TODO combine this with JournalFileImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface JournalFile
{
   int getNegCount(JournalFile file);

   void incNegCount(JournalFile file);

   void decNegCount(JournalFile file);

   /** Total Negative from other files to this file */
   int getTotalNegCount();

   /** Set by the Reclaimer */
   void setTotalNegCount(int total);

   /** 
    * 
    * A list of problematic records that would cause a linked-list effect between two files
    * Information we will need in order to cleanup necessary delete records.
    * 
    * To avoid a linked-list effect, we physically remove deleted records from other files, when cleaning up
    * */
   void addCleanupInfo(long id, JournalFile deleteFile);

   /**
    * A list of problematic records that would cause a linked-list effect between two files
    * @param id
    * @return The list
    */
   JournalFile getCleanupInfo(long id);

   int getPosCount();

   void incPosCount();

   void decPosCount();

   void setCanReclaim(boolean canDelete);
   

   /**
    * Property marking if this file is holding another file from reclaiming because of pending deletes.
    *  */
   void setLinkedDependency(boolean hasDependencies);
   
   /**
    * @see JournalFile#setLinkedDependency(boolean) 
    *  */
   boolean isLinkedDependency();

   boolean isCanReclaim();

   void extendOffset(final int delta);

   long getOffset();

   int getOrderingID();

   void setOffset(final long offset);

   SequentialFile getFile();
}
