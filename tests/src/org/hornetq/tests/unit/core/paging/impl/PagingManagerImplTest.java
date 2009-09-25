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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executors;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.impl.PagedMessageImpl;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.persistence.impl.nullpm.NullStorageManager;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.HierarchicalObjectRepository;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingManagerImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPagingManager() throws Exception
   {
      
      HierarchicalRepository<AddressSettings> addressSettings = new HierarchicalObjectRepository<AddressSettings>();
      addressSettings.setDefault(new AddressSettings());
      
      PagingManagerImpl managerImpl = new PagingManagerImpl(new PagingStoreFactoryNIO(getPageDir(), new OrderedExecutorFactory(Executors.newCachedThreadPool())),
                                                            new NullStorageManager(),
                                                            addressSettings,
                                                            true);

      managerImpl.start();

      TestSupportPageStore store = (TestSupportPageStore)managerImpl.getPageStore(new SimpleString("simple-test"));

      ServerMessage msg = createMessage(1l, new SimpleString("simple-test"), createRandomBuffer(10));

      assertFalse(store.page(new PagedMessageImpl(msg), true, true));

      store.startPaging();

      assertTrue(store.page(new PagedMessageImpl(msg), true, true));

      Page page = store.depage();

      page.open();

      List<PagedMessage> msgs = page.read();

      page.close();

      assertEquals(1, msgs.size());

      assertEqualsByteArrays(msg.getBody().array(), (msgs.get(0).getMessage(null)).getBody().array());

      assertTrue(store.isPaging());

      assertNull(store.depage());

      assertFalse(store.page(new PagedMessageImpl(msg), true, true));
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      recreateDirectory();
   }

   protected ServerMessage createMessage(final long messageId, final SimpleString destination, final ByteBuffer buffer)
   {
      ServerMessage msg = new ServerMessageImpl((byte)1,
                                                true,
                                                0,
                                                System.currentTimeMillis(),
                                                (byte)0,
                                                ChannelBuffers.wrappedBuffer(new byte[1024]));

      msg.setMessageID(messageId);

      msg.setDestination(destination);
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
   protected void tearDown() throws Exception
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
