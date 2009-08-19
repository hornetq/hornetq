/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.hornetq.core.journal.impl;

import org.hornetq.core.journal.RecordInfo;

/**
 * A JournalReader
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public interface JournalReaderCallback
{
   void onReadAddRecord(RecordInfo info) throws Exception;

   /**
    * @param recordInfo
    * @throws Exception 
    */
   void onReadUpdateRecord(RecordInfo recordInfo) throws Exception;

   /**
    * @param recordID
    */
   void onReadDeleteRecord(long recordID) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    * @throws Exception 
    */
   void onReadAddRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    * @throws Exception 
    */
   void onReadUpdateRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param recordInfo
    */
   void onReadDeleteRecordTX(long transactionID, RecordInfo recordInfo) throws Exception;

   /**
    * @param transactionID
    * @param extraData
    * @param summaryData
    */
   void onReadPrepareRecord(long transactionID, byte[] extraData, int numberOfRecords) throws Exception;

   /**
    * @param transactionID
    * @param summaryData
    */
   void onReadCommitRecord(long transactionID, int numberOfRecords) throws Exception;

   /**
    * @param transactionID
    */
   void onReadRollbackRecord(long transactionID) throws Exception;

   public void markAsDataFile(JournalFile file);


}
