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

package org.hornetq.core.postoffice;

import java.util.List;

import org.hornetq.api.core.Pair;
import org.hornetq.core.transaction.Transaction;

/**
 * A DuplicateIDCache
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 8 Dec 2008 16:36:07
 *
 *
 */
public interface DuplicateIDCache
{
   boolean contains(byte[] duplicateID);

   boolean atomicVerify(final byte[] duplID, final Transaction tx) throws Exception;

   void addToCache(byte[] duplicateID) throws Exception;

   void addToCache(byte[] duplicateID, Transaction tx) throws Exception;

   /**
    * it will add the data to the cache.
    * If TX == null it won't use a transaction.
    * if instantAdd=true, it won't wait a transaction to add on the cache which is needed on the case of the Bridges
    */
   void addToCache(byte[] duplicateID, Transaction tx, boolean instantAdd) throws Exception;

   void deleteFromCache(byte [] duplicateID) throws Exception;

   void load(List<Pair<byte[], Long>> theIds) throws Exception;

   void load(final Transaction tx, final byte[] duplID);

   void clear() throws Exception;
}
