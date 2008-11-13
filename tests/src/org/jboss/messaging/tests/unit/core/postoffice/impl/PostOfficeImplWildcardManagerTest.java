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
package org.jboss.messaging.tests.unit.core.postoffice.impl;

import java.util.List;
import java.util.Map;

import org.easymock.EasyMock;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class PostOfficeImplWildcardManagerTest extends PostOfficeImplTest
{
   protected void setUp() throws Exception
   {
      super.setUp();
      wildCardRoutingEnabled = true;
   }

   public void testPostOfficeRouteToWildcardBinding() throws Exception
   {
      SimpleString queueName = new SimpleString("testQ");
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage message2 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage message3 = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference3 = EasyMock.createStrictMock(MessageReference.class);
      SimpleString address = new SimpleString("test.testAddress");
      SimpleString address2 = new SimpleString("test.testAddress2");
      SimpleString address3 = new SimpleString("test.testAddress3");
      Queue queue = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PagingManager pgm = EasyMock.createNiceMock(PagingManager.class);
      EasyMock.expect(pgm.isGlobalPageMode()).andStubReturn(true);

      PostOffice postOffice = new PostOfficeImpl(pm, pgm, qf, ms, true, null, wildCardRoutingEnabled, false);

      qf.setPostOffice(postOffice);

      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject(), (ResourceManager) EasyMock.anyObject());
      EasyMock.expect(pm.addDestination(address)).andReturn(true);
      EasyMock.expect(pm.addDestination(address2)).andReturn(true);
      EasyMock.expect(pm.addDestination(address3)).andReturn(true);
      EasyMock.expect(message.getDestination()).andStubReturn(address);
      EasyMock.expect(message2.getDestination()).andStubReturn(address2);
      EasyMock.expect(message3.getDestination()).andStubReturn(address3);
      EasyMock.expect(qf.createQueue(-1, queueName, null, false, false)).andReturn(queue);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue.getFilter()).andStubReturn(null);
      EasyMock.expect(pgm.addSize(message)).andStubReturn(1000l);
      //this bit is the test itself, if the reference is created for each queue thenwe know that they have been routed via all 3 queues
      EasyMock.expect(message.createReference(queue)).andReturn(messageReference);
      EasyMock.expect(message2.createReference(queue)).andReturn(messageReference2);
      EasyMock.expect(message3.createReference(queue)).andReturn(messageReference3);
      EasyMock.replay(pgm,pm, qf, message, message2, message3, queue);
      postOffice.start();
      assertTrue(postOffice.addDestination(address, true));
      assertTrue(postOffice.addDestination(address2, true));
      assertTrue(postOffice.addDestination(address3, true));
      assertTrue(postOffice.containsDestination(address));
      assertTrue(postOffice.containsDestination(address2));
      assertTrue(postOffice.containsDestination(address3));
      postOffice.addBinding(new SimpleString("test.*"), queueName, null, false, false, true);
      postOffice.route(message);
      postOffice.route(message2);
      postOffice.route(message3);
      EasyMock.verify(pgm, pm, qf, message, message2, message3, queue);
   }

   public void testPostOfficeRouteToMultipleWildcardBinding() throws Exception
   {
      SimpleString queueName = new SimpleString("testQ");
      SimpleString queueName2 = new SimpleString("testQ2");
      ServerMessage message = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage message2 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage message3 = EasyMock.createStrictMock(ServerMessage.class);
      ServerMessage message4 = EasyMock.createStrictMock(ServerMessage.class);
      MessageReference messageReference = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference2 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference3 = EasyMock.createStrictMock(MessageReference.class);
      MessageReference messageReference4 = EasyMock.createStrictMock(MessageReference.class);
      SimpleString address = new SimpleString("test.testAddress");
      SimpleString address2 = new SimpleString("test2.testAddress2");
      SimpleString address3 = new SimpleString("test.testAddress3");
      SimpleString address4 = new SimpleString("test2.testAddress3");
      Queue queue = EasyMock.createStrictMock(Queue.class);
      Queue queue2 = EasyMock.createStrictMock(Queue.class);
      StorageManager pm = EasyMock.createStrictMock(StorageManager.class);
      QueueFactory qf = EasyMock.createStrictMock(QueueFactory.class);
      ManagementService ms = EasyMock.createNiceMock(ManagementService.class);
      PagingManager pgm = EasyMock.createNiceMock(PagingManager.class);

      PostOffice postOffice = new PostOfficeImpl(pm, pgm, qf, ms, true, null, wildCardRoutingEnabled, false);

      qf.setPostOffice(postOffice);

      pm.loadBindings(EasyMock.eq(qf), (List<Binding>) EasyMock.anyObject(), (List<SimpleString>) EasyMock.anyObject());
      pm.loadMessages(EasyMock.eq(postOffice), (Map<Long, Queue>) EasyMock.anyObject(), (ResourceManager) EasyMock.anyObject());
      EasyMock.expect(pm.addDestination(address)).andReturn(true);
      EasyMock.expect(pm.addDestination(address2)).andReturn(true);
      EasyMock.expect(pm.addDestination(address3)).andReturn(true);
      EasyMock.expect(pm.addDestination(address4)).andReturn(true);
      EasyMock.expect(message.getDestination()).andStubReturn(address);
      EasyMock.expect(message2.getDestination()).andStubReturn(address2);
      EasyMock.expect(message3.getDestination()).andStubReturn(address3);
      EasyMock.expect(message4.getDestination()).andStubReturn(address4);
      EasyMock.expect(qf.createQueue(-1, queueName, null, false, false)).andReturn(queue);
      EasyMock.expect(qf.createQueue(-1, queueName2, null, false, false)).andReturn(queue2);
      EasyMock.expect(queue.getName()).andStubReturn(queueName);
      EasyMock.expect(queue2.getName()).andStubReturn(queueName2);
      EasyMock.expect(queue.getFilter()).andStubReturn(null);
      EasyMock.expect(queue2.getFilter()).andStubReturn(null);
      EasyMock.expect(pgm.addSize(message)).andStubReturn(1000l);
      //this bit is the test itself, if the reference is created for each queue then we know that they have been routed via all 3 queues
      EasyMock.expect(message.createReference(queue)).andReturn(messageReference);
      EasyMock.expect(message2.createReference(queue2)).andReturn(messageReference2);
      EasyMock.expect(message3.createReference(queue)).andReturn(messageReference3);
      EasyMock.expect(message4.createReference(queue2)).andReturn(messageReference4);
      EasyMock.replay(pgm,pm, qf, message, message2, message3, message4, queue, queue2);
      postOffice.start();
      postOffice.addDestination(address, true);
      postOffice.addDestination(address2, true);
      postOffice.addDestination(address3, true);
      postOffice.addDestination(address4, true);
      postOffice.addBinding(new SimpleString("test.*"), queueName, null, false, false, true);
      postOffice.addBinding(new SimpleString("test2.*"), queueName2, null, false, false, true);
      postOffice.route(message);
      postOffice.route(message2);
      postOffice.route(message3);
      postOffice.route(message4);
      EasyMock.verify(pgm, pm, qf, message, message2, message3, message4,queue, queue2);
   }
}
