/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.paging;

import org.hornetq.core.journal.EncodingSupport;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PageTransactionInfo extends EncodingSupport
{
   boolean waitCompletion(int timeoutMilliSeconds) throws Exception;

   boolean isCommit();

   boolean isRollback();

   void commit();

   void rollback();

   long getRecordID();

   void setRecordID(long id);

   long getTransactionID();

   int increment();

   int decrement();

   int getNumberOfMessages();

   void markIncomplete();
}
