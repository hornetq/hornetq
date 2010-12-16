/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.paging.cursor;

import org.hornetq.core.transaction.Transaction;

/**
 * A PagingSubscriptionCounterInterface
 *
 * @author clebertsuconic
 *
 *
 */
public interface PageSubscriptionCounter
{

   public abstract long getValue();

   public abstract void increment(Transaction tx, int add) throws Exception;

   public abstract void loadValue(final long recordValueID, final long value);

   public abstract void incrementProcessed(long id, int variance);

   /**
    * 
    * This method is also used by Journal.loadMessageJournal
    * @param id
    * @param variance
    */
   public abstract void addInc(long id, int variance);

}