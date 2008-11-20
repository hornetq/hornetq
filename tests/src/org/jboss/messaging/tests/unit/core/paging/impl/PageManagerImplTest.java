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

import java.util.ArrayList;
import java.util.List;

import org.easymock.EasyMock;
import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.paging.impl.PagedMessageImpl;
import org.jboss.messaging.core.paging.impl.PagingManagerImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.HierarchicalObjectRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

public class PageManagerImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static HierarchicalRepository<QueueSettings> repoSettings = new HierarchicalObjectRepository<QueueSettings>();
   static
   {
      repoSettings.setDefault(new QueueSettings());
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   public void testOnDepage() throws Exception
   {
      long time = System.currentTimeMillis() + 10000;
      List<MessageReference> refs = new ArrayList<MessageReference>();
      MessageReference ref = EasyMock.createStrictMock(MessageReference.class);
      refs.add(ref);
      Queue queue = EasyMock.createStrictMock(Queue.class);
      HierarchicalRepository<QueueSettings> queueSettings = new HierarchicalObjectRepository<QueueSettings>();
      queueSettings.setDefault(new QueueSettings());
      PostOffice po = EasyMock.createStrictMock(PostOffice.class);
      PagingStoreFactory spi = EasyMock.createMock(PagingStoreFactory.class);
      PagingStore store = EasyMock.createNiceMock(PagingStore.class);
      StorageManager storageManager = EasyMock.createStrictMock(StorageManager.class);
      PagingManagerImpl manager = new PagingManagerImpl(spi, storageManager, queueSettings, -1, 1024 * 1024);
      manager.setPostOffice(po);
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);

      EasyMock.expect(storageManager.generateUniqueID()).andReturn(1l);
      EasyMock.expect(po.route(message)).andReturn(refs);
      EasyMock.expect(message.getDurableRefCount()).andReturn(1);
      storageManager.storeLastPage(EasyMock.anyLong(), (LastPageRecord) EasyMock.anyObject());
      storageManager.storeMessageTransactional(EasyMock.anyLong(), (ServerMessage) EasyMock.anyObject());
      storageManager.commit(EasyMock.anyLong());
      EasyMock.expect(ref.getQueue()).andReturn(queue);
      EasyMock.expect(queue.addLast(ref)).andReturn(null);
      EasyMock.replay(spi, store, message, storageManager, po, ref, queue);
      SimpleString queueName = new SimpleString("aq");
      PagedMessageImpl pageMessage = new PagedMessageImpl(message);

      manager.onDepage(0, queueName, store, new PagedMessage[] {pageMessage} );
      EasyMock.verify(spi, store, message, storageManager, po, ref, queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
