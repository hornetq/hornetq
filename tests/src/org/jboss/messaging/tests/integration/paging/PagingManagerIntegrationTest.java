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

import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageMessage;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.impl.PageMessageImpl;
import org.jboss.messaging.core.paging.impl.PagingManagerFactoryNIO;
import org.jboss.messaging.core.paging.impl.PagingManagerImpl;
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

   protected String journalDir = System.getProperty("java.io.tmpdir", "/tmp") +  "/journal-test";
  
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------
   
   public void testPagingManagerNIO() throws Exception
   {
      HierarchicalRepository<QueueSettings> queueSettings = new HierarchicalObjectRepository<QueueSettings>();
      queueSettings.setDefault(new QueueSettings());
      
      
      PagingManagerImpl managerImpl = 
         new PagingManagerImpl(new PagingManagerFactoryNIO(journalDir), null, queueSettings, -1);
      managerImpl.start();
      
      PagingStore store = managerImpl.getPageStore(new SimpleString("simple-test"));
      
      ServerMessage msg = createMessage(1l, new SimpleString("simple-test"), createRandomBuffer(10));
      
      assertFalse(store.page(new PageMessageImpl(msg)));
      
      store.startPaging();
      
      assertTrue(store.page(new PageMessageImpl(msg)));
      
      Page page = store.depage();
      
      page.open();
      
      PageMessage msgs[] = page.read();
      
      page.close();
      
      assertEquals(1, msgs.length);
      
      assertEqualsByteArrays(msg.getBody().array(), msgs[0].getMessage().getBody().array());
      
      assertTrue(store.isPaging());
      
      assertNull(store.depage());
      
      assertFalse(store.page(new PageMessageImpl(msg)));
   }
   
   public void testPagingManagerAddressFull() throws Exception
   {
      HierarchicalRepository<QueueSettings> queueSettings = new HierarchicalObjectRepository<QueueSettings>();
      queueSettings.setDefault(new QueueSettings());
      
      QueueSettings simpleTestSettings = new QueueSettings();
      simpleTestSettings.setDropMessagesWhenFull(true);
      simpleTestSettings.setMaxSizeBytes(200);
      
      queueSettings.addMatch("simple-test", simpleTestSettings);
      
      PagingManagerImpl managerImpl = 
         new PagingManagerImpl(new PagingManagerFactoryNIO(journalDir), null, queueSettings, -1);
      managerImpl.start();
      
      ServerMessage msg = createMessage(1l, new SimpleString("simple-test"), createRandomBuffer(100));
      
      long currentSize = managerImpl.addSize(msg);
      
      assertTrue(currentSize > 0);
      
      assertEquals(currentSize, managerImpl.getPageStore(new SimpleString("simple-test")).getAddressSize());
      
      for (int i = 0; i < 10; i++)
      {
         assertTrue(managerImpl.addSize(msg) < 0);
         
         assertEquals(currentSize, managerImpl.getPageStore(new SimpleString("simple-test")).getAddressSize());
      }
      
      managerImpl.messageDone(msg);
      
      assertTrue(managerImpl.addSize(msg) > 0);
      
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

   protected ServerMessage createMessage(long messageId, SimpleString destination, ByteBuffer buffer)
   {
      ServerMessage msg = new ServerMessageImpl((byte)1, true, 0,
            System.currentTimeMillis(), (byte)0, new ByteBufferWrapper(buffer));
      
      msg.setMessageID((long)messageId);
      
      msg.setDestination(destination);
      return msg;
   }

   protected ByteBuffer createRandomBuffer(int size)
   {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      
      for (int j = 0; j < buffer.limit(); j++)
      {
         buffer.put(RandomUtil.randomByte());
      }
      return buffer;
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      //deleteDirectory(new File(journalDir));
   }
   
   // Private -------------------------------------------------------

   private void recreateDirectory()
   {
      File fileJournalDir = new File(journalDir);
      deleteDirectory(fileJournalDir);
      fileJournalDir.mkdirs();
   }

   
   // Inner classes -------------------------------------------------
   
}
