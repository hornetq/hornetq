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

package org.jboss.messaging.tests.integration.paging;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.impl.PagedMessageImpl;
import org.jboss.messaging.core.paging.impl.PagingManagerImpl;
import org.jboss.messaging.core.paging.impl.PagingStoreFactoryNIO;
import org.jboss.messaging.core.paging.impl.TestSupportPageStore;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.RandomUtil;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingManagerIntegrationTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPagingManager() throws Exception
   {
      HierarchicalRepository<QueueSettings> queueSettings = new HierarchicalObjectRepository<QueueSettings>();
      queueSettings.setDefault(new QueueSettings());

      PagingManagerImpl managerImpl = new PagingManagerImpl(new PagingStoreFactoryNIO(getPageDir(), 10),
                                                            null,
                                                            queueSettings,
                                                            -1,
                                                            1024 * 1024,
                                                            true);

      managerImpl.start();

      TestSupportPageStore store = (TestSupportPageStore)managerImpl.createPageStore(new SimpleString("simple-test"));

      ServerMessage msg = createMessage(1l, new SimpleString("simple-test"), createRandomBuffer(10));

      assertFalse(store.page(new PagedMessageImpl(msg), true));

      store.startPaging();

      assertTrue(store.page(new PagedMessageImpl(msg), true));

      Page page = store.depage();

      page.open();

      List<PagedMessage> msgs = page.read();

      page.close();

      assertEquals(1, msgs.size());

      assertEqualsByteArrays(msg.getBody().array(), (msgs.get(0).getMessage(null)).getBody().array());

      assertTrue(store.isPaging());

      assertNull(store.depage());

      assertFalse(store.page(new PagedMessageImpl(msg), true));
   }


   public void testPagingManagerAddressFull() throws Exception
   {
      HierarchicalRepository<QueueSettings> queueSettings = new HierarchicalObjectRepository<QueueSettings>();
      queueSettings.setDefault(new QueueSettings());

      QueueSettings simpleTestSettings = new QueueSettings();
      simpleTestSettings.setDropMessagesWhenFull(true);
      simpleTestSettings.setMaxSizeBytes(200);

      queueSettings.addMatch("simple-test", simpleTestSettings);

      PagingManagerImpl managerImpl = new PagingManagerImpl(new PagingStoreFactoryNIO(getJournalDir(), 10),
                                                            null,
                                                            queueSettings,
                                                            -1,
                                                            1024 * 1024,
                                                            false);
      managerImpl.start();

      managerImpl.createPageStore(new SimpleString("simple-test"));

      ServerMessage msg = createMessage(1l, new SimpleString("simple-test"), createRandomBuffer(100));

      assertTrue(managerImpl.addSize(msg));

      for (int i = 0; i < 10; i++)
      {
         long currentSize = managerImpl.getPageStore(new SimpleString("simple-test")).getAddressSize();
         assertFalse(managerImpl.addSize(msg));

         // should be unchanged
         assertEquals(currentSize, managerImpl.getPageStore(new SimpleString("simple-test")).getAddressSize());
      }

      managerImpl.messageDone(msg);

      assertTrue(managerImpl.addSize(msg));

      managerImpl.stop();
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
                                                new ByteBufferWrapper(buffer));

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
   }

   // Inner classes -------------------------------------------------

}
