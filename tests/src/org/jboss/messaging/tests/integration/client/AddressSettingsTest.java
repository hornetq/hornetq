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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AddressSettingsTest extends ServiceTestBase
{
   private SimpleString addressA = new SimpleString("addressA");

   private SimpleString addressA2 = new SimpleString("add.addressA");

   private SimpleString addressB = new SimpleString("addressB");

   private SimpleString addressB2 = new SimpleString("add.addressB");

   private SimpleString addressC = new SimpleString("addressC");

   private SimpleString addressC2 = new SimpleString("add.addressC");

   private SimpleString queueA = new SimpleString("queueA");

   private SimpleString queueB = new SimpleString("queueB");

   private SimpleString queueC = new SimpleString("queueC");

   private SimpleString dlaA = new SimpleString("dlaA");

   private SimpleString dlqA = new SimpleString("dlqA");

   private SimpleString dlaB = new SimpleString("dlaB");

   private SimpleString dlqB = new SimpleString("dlqB");

   private SimpleString dlaC = new SimpleString("dlaC");

   private SimpleString dlqC = new SimpleString("dlqC");

   public void testSimpleHierarchyWithDLA() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = messagingService.getServer().getAddressSettingsRepository();
         repos.addMatch(addressA.toString(), addressSettings);
         repos.addMatch(addressB.toString(), addressSettings2);
         ClientSessionFactory sf = createInVMFactory();
         ClientSession session = sf.createSession(false, true, false);
         session.createQueue(addressA, queueA, false);
         session.createQueue(addressB, queueB, false);
         session.createQueue(dlaA, dlqA, false);
         session.createQueue(dlaB, dlqB, false);
         ClientSession sendSession = sf.createSession(false, true, true);
         ClientMessage cm = sendSession.createClientMessage(true);
         cm.getBody().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBody().writeString("B");
         ClientProducer cp1 = sendSession.createProducer(addressA);
         ClientProducer cp2 = sendSession.createProducer(addressB);
         cp1.send(cm);
         cp2.send(cm2);

         ClientConsumer dlqARec = session.createConsumer(dlqA);
         ClientConsumer dlqBrec = session.createConsumer(dlqB);
         ClientConsumer cc1 = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueB);
         session.start();
         ClientMessage message = cc1.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc2.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         session.rollback();
         cc1.close();
         cc2.close();
         message = dlqARec.receive(5000);
         assertNotNull(message);
         assertEquals("A", message.getBody().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBody().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void test2LevelHierarchyWithDLA() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = messagingService.getServer().getAddressSettingsRepository();
         repos.addMatch(addressA.toString(), addressSettings);
         repos.addMatch("#", addressSettings2);
         ClientSessionFactory sf = createInVMFactory();
         ClientSession session = sf.createSession(false, true, false);
         session.createQueue(addressA, queueA, false);
         session.createQueue(addressB, queueB, false);
         session.createQueue(dlaA, dlqA, false);
         session.createQueue(dlaB, dlqB, false);
         ClientSession sendSession = sf.createSession(false, true, true);
         ClientMessage cm = sendSession.createClientMessage(true);
         cm.getBody().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBody().writeString("B");
         ClientProducer cp1 = sendSession.createProducer(addressA);
         ClientProducer cp2 = sendSession.createProducer(addressB);
         cp1.send(cm);
         cp2.send(cm2);

         ClientConsumer dlqARec = session.createConsumer(dlqA);
         ClientConsumer dlqBrec = session.createConsumer(dlqB);
         ClientConsumer cc1 = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueB);
         session.start();
         ClientMessage message = cc1.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc2.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         session.rollback();
         cc1.close();
         cc2.close();
         message = dlqARec.receive(5000);
         assertNotNull(message);
         assertEquals("A", message.getBody().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBody().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void test2LevelWordHierarchyWithDLA() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = messagingService.getServer().getAddressSettingsRepository();
         repos.addMatch(addressA.toString(), addressSettings);
         repos.addMatch("*", addressSettings2);
         ClientSessionFactory sf = createInVMFactory();
         ClientSession session = sf.createSession(false, true, false);
         session.createQueue(addressA, queueA, false);
         session.createQueue(addressB, queueB, false);
         session.createQueue(dlaA, dlqA, false);
         session.createQueue(dlaB, dlqB, false);
         ClientSession sendSession = sf.createSession(false, true, true);
         ClientMessage cm = sendSession.createClientMessage(true);
         cm.getBody().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBody().writeString("B");
         ClientProducer cp1 = sendSession.createProducer(addressA);
         ClientProducer cp2 = sendSession.createProducer(addressB);
         cp1.send(cm);
         cp2.send(cm2);

         ClientConsumer dlqARec = session.createConsumer(dlqA);
         ClientConsumer dlqBrec = session.createConsumer(dlqB);
         ClientConsumer cc1 = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueB);
         session.start();
         ClientMessage message = cc1.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc2.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         session.rollback();
         cc1.close();
         cc2.close();
         message = dlqARec.receive(5000);
         assertNotNull(message);
         assertEquals("A", message.getBody().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBody().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void test3LevelHierarchyWithDLA() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings3 = new AddressSettings();
         addressSettings3.setDeadLetterAddress(dlaC);
         addressSettings3.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = messagingService.getServer().getAddressSettingsRepository();
         repos.addMatch(addressA2.toString(), addressSettings);
         repos.addMatch("add.*", addressSettings2);
         repos.addMatch("#", addressSettings3);
         ClientSessionFactory sf = createInVMFactory();
         ClientSession session = sf.createSession(false, true, false);
         session.createQueue(addressA2, queueA, false);
         session.createQueue(addressB2, queueB, false);
         session.createQueue(addressC, queueC, false);
         session.createQueue(dlaA, dlqA, false);
         session.createQueue(dlaB, dlqB, false);
         session.createQueue(dlaC, dlqC, false);
         ClientSession sendSession = sf.createSession(false, true, true);
         ClientMessage cm = sendSession.createClientMessage(true);
         cm.getBody().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBody().writeString("B");
         ClientMessage cm3 = sendSession.createClientMessage(true);
         cm3.getBody().writeString("C");
         ClientProducer cp1 = sendSession.createProducer(addressA2);
         ClientProducer cp2 = sendSession.createProducer(addressB2);
         ClientProducer cp3 = sendSession.createProducer(addressC);
         cp1.send(cm);
         cp2.send(cm2);
         cp3.send(cm3);

         ClientConsumer dlqARec = session.createConsumer(dlqA);
         ClientConsumer dlqBrec = session.createConsumer(dlqB);
         ClientConsumer dlqCrec = session.createConsumer(dlqC);
         ClientConsumer cc1 = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueB);
         ClientConsumer cc3 = session.createConsumer(queueC);
         session.start();
         ClientMessage message = cc1.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc2.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc3.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         session.rollback();
         cc1.close();
         cc2.close();
         cc3.close();
         message = dlqARec.receive(5000);
         assertNotNull(message);
         assertEquals("A", message.getBody().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBody().readString());
         message = dlqCrec.receive(5000);
         assertNotNull(message);
         assertEquals("C", message.getBody().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testOverrideHierarchyWithDLA() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings3 = new AddressSettings();
         addressSettings3.setDeadLetterAddress(dlaC);
         addressSettings3.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = messagingService.getServer().getAddressSettingsRepository();
         repos.addMatch(addressA2.toString(), addressSettings);
         repos.addMatch("add.*", addressSettings2);
         repos.addMatch("#", addressSettings3);
         ClientSessionFactory sf = createInVMFactory();
         ClientSession session = sf.createSession(false, true, false);
         session.createQueue(addressA2, queueA, false);
         session.createQueue(addressB2, queueB, false);
         session.createQueue(addressC, queueC, false);
         session.createQueue(dlaA, dlqA, false);
         session.createQueue(dlaB, dlqB, false);
         session.createQueue(dlaC, dlqC, false);
         ClientSession sendSession = sf.createSession(false, true, true);
         ClientMessage cm = sendSession.createClientMessage(true);
         ClientMessage cm2 = sendSession.createClientMessage(true);
         ClientMessage cm3 = sendSession.createClientMessage(true);
         ClientProducer cp1 = sendSession.createProducer(addressA2);
         ClientProducer cp2 = sendSession.createProducer(addressB2);
         ClientProducer cp3 = sendSession.createProducer(addressC);
         cp1.send(cm);
         cp2.send(cm2);
         cp3.send(cm3);

         ClientConsumer dlqCrec = session.createConsumer(dlqC);
         ClientConsumer cc1 = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueB);
         ClientConsumer cc3 = session.createConsumer(queueC);
         session.start();
         ClientMessage message = cc1.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc2.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         message = cc3.receive(5000);
         assertNotNull(message);
         message.acknowledge();
         session.rollback();
         cc1.close();
         cc2.close();
         cc3.close();
         message = dlqCrec.receive(5000);
         assertNotNull(message);
         message = dlqCrec.receive(5000);
         assertNotNull(message);
         message = dlqCrec.receive(5000);
         assertNotNull(message);
         sendSession.close();
         session.close();
      }
      finally
      {
         if(messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }
}
