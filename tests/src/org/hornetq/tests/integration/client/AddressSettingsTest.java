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
package org.hornetq.tests.integration.client;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;
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
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
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
         cm.getBodyBuffer().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBodyBuffer().writeString("B");
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
         assertEquals("A", message.getBodyBuffer().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBodyBuffer().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void test2LevelHierarchyWithDLA() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
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
         cm.getBodyBuffer().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBodyBuffer().writeString("B");
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
         assertEquals("A", message.getBodyBuffer().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBodyBuffer().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void test2LevelWordHierarchyWithDLA() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
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
         cm.getBodyBuffer().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBodyBuffer().writeString("B");
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
         assertEquals("A", message.getBodyBuffer().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBodyBuffer().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void test3LevelHierarchyWithDLA() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setDeadLetterAddress(dlaA);
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setDeadLetterAddress(dlaB);
         addressSettings2.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings3 = new AddressSettings();
         addressSettings3.setDeadLetterAddress(dlaC);
         addressSettings3.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
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
         cm.getBodyBuffer().writeString("A");
         ClientMessage cm2 = sendSession.createClientMessage(true);
         cm2.getBodyBuffer().writeString("B");
         ClientMessage cm3 = sendSession.createClientMessage(true);
         cm3.getBodyBuffer().writeString("C");
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
         assertEquals("A", message.getBodyBuffer().readString());
         message = dlqBrec.receive(5000);
         assertNotNull(message);
         assertEquals("B", message.getBodyBuffer().readString());
         message = dlqCrec.receive(5000);
         assertNotNull(message);
         assertEquals("C", message.getBodyBuffer().readString());
         sendSession.close();
         session.close();
      }
      finally
      {
         if(server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testOverrideHierarchyWithDLA() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         AddressSettings addressSettings = new AddressSettings();
         addressSettings.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings2 = new AddressSettings();
         addressSettings2.setMaxDeliveryAttempts(1);
         AddressSettings addressSettings3 = new AddressSettings();
         addressSettings3.setDeadLetterAddress(dlaC);
         addressSettings3.setMaxDeliveryAttempts(1);
         HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
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
         if(server.isStarted())
         {
            server.stop();
         }
      }
   }
}
