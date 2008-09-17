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

package org.jboss.messaging.tests.unit.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageMessage;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.impl.PageMessageImpl;
import org.jboss.messaging.core.paging.impl.PagingStoreImpl;
import org.jboss.messaging.core.paging.impl.TestSupportPageStore;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.unit.core.journal.impl.fakes.FakeSequentialFileFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreImplTest extends PagingStoreTestBase
{

   // Constants -----------------------------------------------------

   private final static SimpleString destinationTestName = new SimpleString("test");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDoubleStart() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStore storeImpl = new PagingStoreImpl(null, factory, destinationTestName, new QueueSettings(), executor);

      storeImpl.start();

      // this is not supposed to throw an exception.
      // As you could have start being called twice as Stores are dynamically
      // created, on a multi-thread environment
      storeImpl.start();

      storeImpl.stop();

   }

   public void testStore() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStore storeImpl = new PagingStoreImpl(null, factory, destinationTestName, new QueueSettings(), executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

      ByteBuffer buffer = createRandomBuffer(0, 10);

      buffers.add(buffer);
      SimpleString destination = new SimpleString("test");

      PageMessageImpl msg = createMessage(destination, buffer);

      assertTrue(storeImpl.isPaging());

      assertTrue(storeImpl.page(msg));

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      storeImpl = new PagingStoreImpl(null, factory, destinationTestName, new QueueSettings(), executor);

      storeImpl.start();

      assertEquals(2, storeImpl.getNumberOfPages());

   }

   public void testDepageOnCurrentPage() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      PagingStore storeImpl = new PagingStoreImpl(null, factory, destinationTestName, new QueueSettings(), executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

      SimpleString destination = new SimpleString("test");

      for (int i = 0; i < 10; i++)
      {

         ByteBuffer buffer = createRandomBuffer(i + 1l, 10);

         buffers.add(buffer);

         PageMessageImpl msg = createMessage(destination, buffer);

         assertTrue(storeImpl.page(msg));
      }

      assertEquals(1, storeImpl.getNumberOfPages());

      storeImpl.sync();

      Page page = storeImpl.depage();

      page.open();

      PageMessage msg[] = page.read();

      assertEquals(10, msg.length);
      assertEquals(1, storeImpl.getNumberOfPages());

      page = storeImpl.depage();

      assertNull(page);

      assertEquals(0, storeImpl.getNumberOfPages());

      for (int i = 0; i < 10; i++)
      {
         assertEquals(0, msg[i].getMessage().getMessageID());
         assertEqualsByteArrays(buffers.get(i).array(), msg[i].getMessage().getBody().array());
      }

   }

   public void testDepageMultiplePages() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory();

      TestSupportPageStore storeImpl = new PagingStoreImpl(null,
                                                           factory,
                                                           destinationTestName,
                                                           new QueueSettings(),
                                                           executor);

      storeImpl.start();

      assertEquals(0, storeImpl.getNumberOfPages());

      storeImpl.startPaging();

      assertEquals(1, storeImpl.getNumberOfPages());

      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

      SimpleString destination = new SimpleString("test");

      for (int i = 0; i < 10; i++)
      {

         ByteBuffer buffer = createRandomBuffer(i + 1l, 10);

         buffers.add(buffer);

         if (i == 5)
         {
            storeImpl.forceAnotherPage();
         }

         PageMessageImpl msg = createMessage(destination, buffer);

         assertTrue(storeImpl.page(msg));
      }

      assertEquals(2, storeImpl.getNumberOfPages());

      storeImpl.sync();

      for (int pageNr = 0; pageNr < 2; pageNr++)
      {
         Page page = storeImpl.depage();

         page.open();

         PageMessage msg[] = page.read();

         page.close();

         assertEquals(5, msg.length);

         for (int i = 0; i < 5; i++)
         {
            assertEquals(0, msg[i].getMessage().getMessageID());
            assertEqualsByteArrays(buffers.get(pageNr * 5 + i).array(), msg[i].getMessage().getBody().array());
         }
      }

      assertEquals(1, storeImpl.getNumberOfPages());

      assertTrue(storeImpl.isPaging());

      PageMessageImpl msg = createMessage(destination, buffers.get(0));

      assertTrue(storeImpl.page(msg));

      Page newPage = storeImpl.depage();

      newPage.open();

      assertEquals(1, newPage.read().length);

      newPage.delete();

      assertEquals(1, storeImpl.getNumberOfPages());

      assertTrue(storeImpl.isPaging());

      assertNull(storeImpl.depage());

      assertFalse(storeImpl.isPaging());

      assertFalse(storeImpl.page(msg));

      storeImpl.startPaging();

      assertTrue(storeImpl.page(msg));

      Page page = storeImpl.depage();

      page.open();

      PageMessage msgs[] = page.read();

      assertEquals(1, msgs.length);

      assertEquals(0l, msgs[0].getMessage().getMessageID());

      assertEqualsByteArrays(buffers.get(0).array(), msgs[0].getMessage().getBody().array());

      assertEquals(1, storeImpl.getNumberOfPages());

      assertTrue(storeImpl.isPaging());

      assertNull(storeImpl.depage());

      assertEquals(0, storeImpl.getNumberOfPages());

      page.open();

   }

   public void testConcurrentDepage() throws Exception
   {
      SequentialFileFactory factory = new FakeSequentialFileFactory(1, false);

      testConcurrentPaging(factory, 10);

   }

   // Protected ----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Inner classes -------------------------------------------------

}
