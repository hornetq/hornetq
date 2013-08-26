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

package org.hornetq.tests.unit.core.paging.impl;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.impl.Page;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.OrderedExecutorFactory;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingManagerImplTest extends UnitTestCase
{
   private final ReadLock lock = new ReentrantReadWriteLock().readLock();

   @Test
   public void testPagingManager() throws Exception
   {

      HierarchicalRepository<AddressSettings> addressSettings = new HierarchicalObjectRepository<AddressSettings>();
      AddressSettings settings = new AddressSettings();
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      addressSettings.setDefault(settings);

      final StorageManager storageManager = new NullStorageManager();

      PagingStoreFactoryNIO storeFactory =
               new PagingStoreFactoryNIO(storageManager, getPageDir(), 100, null,
                                         new OrderedExecutorFactory(Executors.newCachedThreadPool()), true, null);

      PagingManagerImpl managerImpl = new PagingManagerImpl(storeFactory, addressSettings);

      managerImpl.start();

      PagingStore store = managerImpl.getPageStore(new SimpleString("simple-test"));

      ServerMessage msg = createMessage(1l, new SimpleString("simple-test"), createRandomBuffer(10));

      final RoutingContextImpl ctx = new RoutingContextImpl(null);
      Assert.assertFalse(store.page(msg, ctx.getTransaction(), ctx.getContextListing(store.getStoreName()), lock));

      store.startPaging();

      Assert.assertTrue(store.page(msg, ctx.getTransaction(), ctx.getContextListing(store.getStoreName()), lock));

      Page page = store.depage();

      page.open();

      List<PagedMessage> msgs = page.read(new NullStorageManager());

      page.close();

      Assert.assertEquals(1, msgs.size());

      UnitTestCase.assertEqualsByteArrays(msg.getBodyBuffer().writerIndex(), msg.getBodyBuffer().toByteBuffer().array(), msgs.get(0)
                                                                                          .getMessage()
                                                                                          .getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array());

      Assert.assertTrue(store.isPaging());

      Assert.assertNull(store.depage());

      final RoutingContextImpl ctx2 = new RoutingContextImpl(null);
      Assert.assertFalse(store.page(msg, ctx2.getTransaction(), ctx2.getContextListing(store.getStoreName()), lock));

   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      recreateDirectory();
   }

   protected ServerMessage createMessage(final long messageId, final SimpleString destination, final ByteBuffer buffer)
   {
      ServerMessage msg = new ServerMessageImpl(messageId, 200);

      msg.setAddress(destination);

      msg.getBodyBuffer().writeBytes(buffer);

      return msg;
   }

   protected ByteBuffer createRandomBuffer(final int size)
   {
      ByteBuffer buffer = ByteBuffer.allocate(size);

      for (int j = 0; j < buffer.limit(); j++)
      {
         buffer.put(RandomUtil.randomByte());
      }
      return buffer;
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
      // deleteDirectory(new File(journalDir));
   }

   // Private -------------------------------------------------------

   private void recreateDirectory()
   {
      File fileJournalDir = new File(getJournalDir());
      deleteDirectory(fileJournalDir);
      fileJournalDir.mkdirs();

      File pageDirDir = new File(getPageDir());
      deleteDirectory(pageDirDir);
      pageDirDir.mkdirs();
   }

   // Inner classes -------------------------------------------------

}
