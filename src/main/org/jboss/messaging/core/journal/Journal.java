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

package org.jboss.messaging.core.journal;

import java.util.List;

import org.jboss.messaging.core.server.MessagingComponent;

/**
 * 
 * A Journal
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface Journal extends MessagingComponent
{
   // Non transactional operations

   void appendAddRecord(long id, byte recordType, EncodingSupport record) throws Exception;

   void appendUpdateRecord(long id, byte recordType, EncodingSupport record) throws Exception;

   void appendDeleteRecord(long id) throws Exception;

   // Transactional operations

   void appendAddRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendUpdateRecordTransactional(long txID, long id, byte recordType, EncodingSupport record) throws Exception;

   void appendDeleteRecordTransactional(long txID, long id, EncodingSupport record) throws Exception;

   void appendCommitRecord(long txID) throws Exception;

   /** 
    * 
    * <p>If the system crashed after a prepare was called, it should store information that is required to bring the transaction 
    *     back to a state it could be committed. </p>
    * 
    * <p> transactionData allows you to store any other supporting user-data related to the transaction</p>
    * 
    * @param txID
    * @param transactionData - extra user data for the prepare
    * @throws Exception
    */
   void appendPrepareRecord(long txID, EncodingSupport transactionData) throws Exception;

   void appendRollbackRecord(long txID) throws Exception;

   // Load

   long load(List<RecordInfo> committedRecords, List<PreparedTransactionInfo> preparedTransactions) throws Exception;

   int getAlignment() throws Exception;

}
