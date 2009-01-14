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
package org.jboss.messaging.tests.integration.wildcard;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class WildCardRoutingTest extends UnitTestCase
{
   private MessagingService messagingService;

   private ClientSession clientSession;

   public void testBasicWildcardRouting() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testBasicWildcardRoutingQueuesDontExist() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);

      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(address).getBindings().size());
   }

   public void testBasicWildcardRoutingQueuesDontExist2() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName = new SimpleString("Q");
      SimpleString queueName2 = new SimpleString("Q2");
      clientSession.createQueue(address, queueName, null, false, false);
      clientSession.createQueue(address, queueName2, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);

      assertEquals(1, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      assertEquals(1, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      assertEquals(1, messagingService.getServer().getPostOffice().getBindingsForAddress(address).getBindings().size());

      clientSession.deleteQueue(queueName2);

      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(address).getBindings().size());
   }

   public void testBasicWildcardRoutingWithHash() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingDestinationsAdded() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      messagingService.getServer().getPostOffice().addDestination(addressAB, false);
      messagingService.getServer().getPostOffice().addDestination(addressAC, false);
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingQueuesAddedAfter() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingQueuesAddedThenDeleted() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      clientSession.deleteQueue(queueName1);
      //the wildcard binding should still exist
      assertEquals(messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size(), 1);
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
      assertEquals(messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size(), 0);
   }

   public void testWildcardRoutingLotsOfQueuesAddedThenDeleted() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString addressAD = new SimpleString("a.d");
      SimpleString addressAE = new SimpleString("a.e");
      SimpleString addressAF = new SimpleString("a.f");
      SimpleString addressAG = new SimpleString("a.g");
      SimpleString addressAH = new SimpleString("a.h");
      SimpleString addressAJ = new SimpleString("a.j");
      SimpleString addressAK = new SimpleString("a.k");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName3 = new SimpleString("Q3");
      SimpleString queueName4 = new SimpleString("Q4");
      SimpleString queueName5 = new SimpleString("Q5");
      SimpleString queueName6 = new SimpleString("Q6");
      SimpleString queueName7 = new SimpleString("Q7");
      SimpleString queueName8 = new SimpleString("Q8");
      SimpleString queueName9 = new SimpleString("Q9");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(addressAD, queueName3, null, false, false);
      clientSession.createQueue(addressAE, queueName4, null, false, false);
      clientSession.createQueue(addressAF, queueName5, null, false, false);
      clientSession.createQueue(addressAG, queueName6, null, false, false);
      clientSession.createQueue(addressAH, queueName7, null, false, false);
      clientSession.createQueue(addressAJ, queueName8, null, false, false);
      clientSession.createQueue(addressAK, queueName9, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer();
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(addressAB, createTextMessage("m1", clientSession));
      producer.send(addressAC, createTextMessage("m2", clientSession));
      producer.send(addressAD, createTextMessage("m3", clientSession));
      producer.send(addressAE, createTextMessage("m4", clientSession));
      producer.send(addressAF, createTextMessage("m5", clientSession));
      producer.send(addressAG, createTextMessage("m6", clientSession));
      producer.send(addressAH, createTextMessage("m7", clientSession));
      producer.send(addressAJ, createTextMessage("m8", clientSession));
      producer.send(addressAK, createTextMessage("m9", clientSession));

      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m3", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m4", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m5", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m6", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m7", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m8", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m9", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
      //now remove all the queues
      clientSession.deleteQueue(queueName1);
      clientSession.deleteQueue(queueName2);
      clientSession.deleteQueue(queueName3);
      clientSession.deleteQueue(queueName4);
      clientSession.deleteQueue(queueName5);
      clientSession.deleteQueue(queueName6);
      clientSession.deleteQueue(queueName7);
      clientSession.deleteQueue(queueName8);
      clientSession.deleteQueue(queueName9);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
   }

   public void testWildcardRoutingLotsOfQueuesAddedThenDeletedHash() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString addressAD = new SimpleString("a.d");
      SimpleString addressAE = new SimpleString("a.e");
      SimpleString addressAF = new SimpleString("a.f");
      SimpleString addressAG = new SimpleString("a.g");
      SimpleString addressAH = new SimpleString("a.h");
      SimpleString addressAJ = new SimpleString("a.j");
      SimpleString addressAK = new SimpleString("a.k");
      SimpleString address = new SimpleString("#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName3 = new SimpleString("Q3");
      SimpleString queueName4 = new SimpleString("Q4");
      SimpleString queueName5 = new SimpleString("Q5");
      SimpleString queueName6 = new SimpleString("Q6");
      SimpleString queueName7 = new SimpleString("Q7");
      SimpleString queueName8 = new SimpleString("Q8");
      SimpleString queueName9 = new SimpleString("Q9");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(addressAD, queueName3, null, false, false);
      clientSession.createQueue(addressAE, queueName4, null, false, false);
      clientSession.createQueue(addressAF, queueName5, null, false, false);
      clientSession.createQueue(addressAG, queueName6, null, false, false);
      clientSession.createQueue(addressAH, queueName7, null, false, false);
      clientSession.createQueue(addressAJ, queueName8, null, false, false);
      clientSession.createQueue(addressAK, queueName9, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer();
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(addressAB, createTextMessage("m1", clientSession));
      producer.send(addressAC, createTextMessage("m2", clientSession));
      producer.send(addressAD, createTextMessage("m3", clientSession));
      producer.send(addressAE, createTextMessage("m4", clientSession));
      producer.send(addressAF, createTextMessage("m5", clientSession));
      producer.send(addressAG, createTextMessage("m6", clientSession));
      producer.send(addressAH, createTextMessage("m7", clientSession));
      producer.send(addressAJ, createTextMessage("m8", clientSession));
      producer.send(addressAK, createTextMessage("m9", clientSession));

      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m3", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m4", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m5", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m6", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m7", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m8", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m9", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
      //now remove all the queues
      clientSession.deleteQueue(queueName1);
      clientSession.deleteQueue(queueName2);
      clientSession.deleteQueue(queueName3);
      clientSession.deleteQueue(queueName4);
      clientSession.deleteQueue(queueName5);
      clientSession.deleteQueue(queueName6);
      clientSession.deleteQueue(queueName7);
      clientSession.deleteQueue(queueName8);
      clientSession.deleteQueue(queueName9);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
   }


   public void testWildcardRoutingWithSingleHash() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingWithHash() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.f");
      SimpleString addressAC = new SimpleString("a.c.f");
      SimpleString address = new SimpleString("a.#.f");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingWithHashMultiLengthAddresses() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c.f");
      SimpleString addressAC = new SimpleString("a.c.f");
      SimpleString addressAD = new SimpleString("a.d");
      SimpleString address = new SimpleString("a.#.f");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingWithDoubleStar() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingPartialMatchStar() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.b");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingVariableLengths() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
   }

   public void testWildcardRoutingVariableLengthsStar() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingMultipleStars() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingStarInMiddle() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.b.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingStarAndHash() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c.d");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("*.b.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testWildcardRoutingHashAndStar() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("#.b.*");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
   }

   public void testLargeWildcardRouting() throws Exception
   {
      SimpleString addressAB = new SimpleString("a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z");
      SimpleString addressAC = new SimpleString("a.c");
      SimpleString address = new SimpleString("a.#");
      SimpleString queueName1 = new SimpleString("Q1");
      SimpleString queueName2 = new SimpleString("Q2");
      SimpleString queueName = new SimpleString("Q");
      clientSession.createQueue(addressAB, queueName1, null, false, false);
      clientSession.createQueue(addressAC, queueName2, null, false, false);
      clientSession.createQueue(address, queueName, null, false, false);
      assertEquals(2, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      assertEquals(2, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      assertEquals(1, messagingService.getServer().getPostOffice().getBindingsForAddress(address).getBindings().size());
      ClientProducer producer = clientSession.createProducer(addressAB);
      ClientProducer producer2 = clientSession.createProducer(addressAC);
      ClientConsumer clientConsumer = clientSession.createConsumer(queueName);
      clientSession.start();
      producer.send(createTextMessage("m1", clientSession));
      producer2.send(createTextMessage("m2", clientSession));
      ClientMessage m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m1", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNotNull(m);
      assertEquals("m2", m.getBody().getString());
      m.acknowledge();
      m = clientConsumer.receive(500);
      assertNull(m);
      clientConsumer.close();
      clientSession.deleteQueue(queueName);
      assertEquals(1, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAB).getBindings().size());
      assertEquals(1, messagingService.getServer().getPostOffice().getBindingsForAddress(addressAC).getBindings().size());
      assertEquals(0, messagingService.getServer().getPostOffice().getBindingsForAddress(address).getBindings().size());
   }

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setWildcardRoutingEnabled(true);
      configuration.setSecurityEnabled(false);
      configuration.setTransactionTimeoutScanPeriod(500);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNullStorageMessagingService(configuration);
      //start the server
      messagingService.start();
      messagingService.getServer().getManagementService().enableNotifications(false);
      //then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      clientSession = sessionFactory.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (MessagingException e1)
         {
            //
         }
      }
      if (messagingService != null && messagingService.isStarted())
      {
         try
         {
            messagingService.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      messagingService = null;
      clientSession = null;
   }
}
