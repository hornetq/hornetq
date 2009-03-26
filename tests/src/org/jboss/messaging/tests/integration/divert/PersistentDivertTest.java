/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.integration.divert;

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DivertConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * A PersistentDivertTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 14 Jan 2009 14:05:01
 *
 *
 */
public class PersistentDivertTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(DivertTest.class);

   public void testPersistentDivert() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      conf.setClustered(true);
      
      final String testAddress = "testAddress";
      
      final String forwardAddress1 = "forwardAddress1";
      
      final String forwardAddress2 = "forwardAddress2";
      
      final String forwardAddress3 = "forwardAddress3";
      
      DivertConfiguration divertConf1 = new DivertConfiguration("divert1", "divert1", testAddress, forwardAddress1, false, null, null);
      
      DivertConfiguration divertConf2 = new DivertConfiguration("divert2", "divert2", testAddress, forwardAddress2, false, null, null);
      
      DivertConfiguration divertConf3 = new DivertConfiguration("divert3", "divert3", testAddress, forwardAddress3, false, null, null);
      
      List<DivertConfiguration> divertConfs = new ArrayList<DivertConfiguration>();
      
      divertConfs.add(divertConf1);
      divertConfs.add(divertConf2);
      divertConfs.add(divertConf3);
      
      conf.setDivertConfigurations(divertConfs);
      
      MessagingService messagingService = Messaging.newMessagingService(conf);
           
      messagingService.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);
      
      final SimpleString queueName1 = new SimpleString("queue1");
      
      final SimpleString queueName2 = new SimpleString("queue2");
      
      final SimpleString queueName3 = new SimpleString("queue3");
      
      final SimpleString queueName4 = new SimpleString("queue4");
      
      session.createQueue(new SimpleString(forwardAddress1), queueName1, null, true);
      
      session.createQueue(new SimpleString(forwardAddress2), queueName2, null, true);
      
      session.createQueue(new SimpleString(forwardAddress3), queueName3, null, true);
      
      session.createQueue(new SimpleString(testAddress), queueName4, null, true);

      session.start();

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);
      
      ClientConsumer consumer2 = session.createConsumer(queueName2);
      
      ClientConsumer consumer3 = session.createConsumer(queueName3);
      
      ClientConsumer consumer4 = session.createConsumer(queueName4);
      
      final int numMessages = 10;
      
      final SimpleString propKey = new SimpleString("testkey");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer1.receive(200));
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer2.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer2.receive(200));
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer3.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer3.receive(200));
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer4.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer4.receive(200));
                  
      session.close();
      
      sf.close();
      
      messagingService.stop();
   }
   
   public void testPersistentDivertRestartBeforeConsume() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      conf.setClustered(true);
      
      final String testAddress = "testAddress";
      
      final String forwardAddress1 = "forwardAddress1";
      
      final String forwardAddress2 = "forwardAddress2";
      
      final String forwardAddress3 = "forwardAddress3";
      
      DivertConfiguration divertConf1 = new DivertConfiguration("divert1", "divert1", testAddress, forwardAddress1, false, null, null);
      
      DivertConfiguration divertConf2 = new DivertConfiguration("divert2", "divert2", testAddress, forwardAddress2, false, null, null);
      
      DivertConfiguration divertConf3 = new DivertConfiguration("divert3", "divert3", testAddress, forwardAddress3, false, null, null);
      
      List<DivertConfiguration> divertConfs = new ArrayList<DivertConfiguration>();
      
      divertConfs.add(divertConf1);
      divertConfs.add(divertConf2);
      divertConfs.add(divertConf3);
      
      conf.setDivertConfigurations(divertConfs);
      
      MessagingService messagingService = Messaging.newMessagingService(conf);
           
      messagingService.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setBlockOnPersistentSend(true);

      ClientSession session = sf.createSession(false, true, true);
      
      final SimpleString queueName1 = new SimpleString("queue1");
      
      final SimpleString queueName2 = new SimpleString("queue2");
      
      final SimpleString queueName3 = new SimpleString("queue3");
      
      final SimpleString queueName4 = new SimpleString("queue4");
      
      session.createQueue(new SimpleString(forwardAddress1), queueName1, null, true);
      
      session.createQueue(new SimpleString(forwardAddress2), queueName2, null, true);
      
      session.createQueue(new SimpleString(forwardAddress3), queueName3, null, true);
      
      session.createQueue(new SimpleString(testAddress), queueName4, null, true);

      ClientProducer producer = session.createProducer(new SimpleString(testAddress));
           
      final int numMessages = 10;
      
      final SimpleString propKey = new SimpleString("testkey");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
      
      session.close();
      
      sf.close();
      
      messagingService.stop();
      
      messagingService.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setBlockOnPersistentSend(true);

      session = sf.createSession(false, true, true);
      
      session.start();
      
      ClientConsumer consumer1 = session.createConsumer(queueName1);
      
      ClientConsumer consumer2 = session.createConsumer(queueName2);
      
      ClientConsumer consumer3 = session.createConsumer(queueName3);
      
      ClientConsumer consumer4 = session.createConsumer(queueName4);
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer1.receive(200));
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer2.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer2.receive(200));
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer3.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer3.receive(200));
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer4.receive(200);
         
         assertNotNull(message);
         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));
         
         message.acknowledge();
      }
      
      assertNull(consumer4.receive(200));
                 
      session.close();
      
      sf.close();
      
      messagingService.stop();
      
      messagingService.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setBlockOnPersistentSend(true);

      session = sf.createSession(false, true, true);
      
      consumer1 = session.createConsumer(queueName1);
      
      consumer2 = session.createConsumer(queueName2);
      
      consumer3 = session.createConsumer(queueName3);
      
      consumer4 = session.createConsumer(queueName4);
            
      assertNull(consumer1.receive(200));
      
      assertNull(consumer2.receive(200));
      
      assertNull(consumer3.receive(200));
      
      assertNull(consumer4.receive(200));
      
      session.close();
      
      sf.close();
      
      assertEquals(0, messagingService.getServer().getPostOffice().getPagingManager().getGlobalSize());
      
      messagingService.stop();
   }
   

}

