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

package org.hornetq.tests.integration.persistence;

import org.junit.Test;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.impl.DuplicateIDCacheImpl;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.tests.util.RandomUtil;

/**
 * A DuplicateCacheTest
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class DuplicateCacheTest extends StorageManagerTestBase
{

   @Test
   public void testDuplicate() throws Exception
   {
      createStorage();

      DuplicateIDCache cache = new DuplicateIDCacheImpl(new SimpleString("test"), 2000, journal, true);

      TransactionImpl tx = new TransactionImpl(journal);

      for (int i = 0 ; i < 5000; i++)
      {
         byte [] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, tx);
      }

      tx.commit();

      tx = new TransactionImpl(journal);

      for (int i = 0 ; i < 5000; i++)
      {
         byte [] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, tx);
      }

      tx.commit();

      byte[] id = RandomUtil.randomBytes();

      assertFalse(cache.contains(id));

      cache.addToCache(id, null);

      assertTrue(cache.contains(id));

      cache.deleteFromCache(id);

      assertFalse(cache.contains(id));

      cache.deleteFromCache(id);

   }


   @Test
   public void testDuplicateNonPersistent() throws Exception
   {
      createStorage();

      DuplicateIDCache cache = new DuplicateIDCacheImpl(new SimpleString("test"), 2000, journal, false);

      TransactionImpl tx = new TransactionImpl(journal);

      for (int i = 0 ; i < 5000; i++)
      {
         byte [] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, tx);
      }

      tx.commit();

      for (int i = 0 ; i < 5000; i++)
      {
         byte [] bytes = RandomUtil.randomBytes();

         cache.addToCache(bytes, null);
      }

   }
}
