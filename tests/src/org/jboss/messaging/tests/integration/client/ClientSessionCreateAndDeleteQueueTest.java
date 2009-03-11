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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.impl.SoloQueueImpl;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientSessionCreateAndDeleteQueueTest extends ServiceTestBase
{
   private MessagingService messagingService;

   private SimpleString address = new SimpleString("address");

   private SimpleString queueName = new SimpleString("queue");


   public void testDurableFalse() throws Exception
   {
      ClientSession session = createInVMFactory().createSession(false, true, true);
      session.createQueue(address, queueName, false);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      assertFalse(q.isDurable());
      
      session.close();
   }

   public void testDurableTrue() throws Exception
   {
      ClientSession session = createInVMFactory().createSession(false, true, true);
      session.createQueue(address, queueName, true);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      assertTrue(q.isDurable());

      session.close();
   }

   public void testTemporaryFalse() throws Exception
   {
      ClientSession session = createInVMFactory().createSession(false, true, true);
      session.createQueue(address, queueName, false, false);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      assertFalse(q.isTemporary());
      
      session.close();
   }

   public void testTemporaryTrue() throws Exception
   {
      ClientSession session = createInVMFactory().createSession(false, true, true);
      session.createQueue(address, queueName, true, true);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      assertTrue(q.isTemporary());
      
      session.close();
   }

   public void testcreateWithFilter() throws Exception
   {
      ClientSession session = createInVMFactory().createSession(false, true, true);
      SimpleString filterString = new SimpleString("x=y");
      session.createQueue(address, queueName, filterString, false, false);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      Queue q = (Queue) binding.getBindable();
      assertEquals(q.getFilter().getFilterString(), filterString);
      
      session.close();
   }

    public void testAddressSettingUSed() throws Exception
   {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setSoloQueue(true);
      messagingService.getServer().getAddressSettingsRepository().addMatch(address.toString(), addressSettings);
      ClientSession session = createInVMFactory().createSession(false, true, true);
      SimpleString filterString = new SimpleString("x=y");
      session.createQueue(address, queueName, filterString, false, false);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      assertTrue(binding.getBindable() instanceof SoloQueueImpl);

      session.close();
   }

   public void testDeleteQueue() throws Exception
   {
      ClientSession session = createInVMFactory().createSession(false, true, true);
      session.createQueue(address, queueName, false);
      Binding binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      assertNotNull(binding);
      session.deleteQueue(queueName);
      binding = messagingService.getServer().getPostOffice().getBinding(queueName);
      assertNull(binding);
      session.close();
   }

   public void testDeleteQueueNotExist() throws Exception
  {
     ClientSession session = createInVMFactory().createSession(false, true, true);
     try
     {
        session.deleteQueue(queueName);
        fail("should throw exception");
     }
     catch (MessagingException e)
     {
        assertEquals(MessagingException.QUEUE_DOES_NOT_EXIST, e.getCode());
     }
     session.close();
  }


   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      messagingService = createService(false);
      messagingService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if(messagingService != null && messagingService.isStarted())
      {
         messagingService.stop();
      }
      
      super.tearDown();

   }
}
