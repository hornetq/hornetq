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


package org.hornetq.tests.integration.server;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * A PredefinedQueueTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 19 Jan 2009 15:44:52
 *
 *
 */
public class PredefinedQueueTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(PredefinedQueueTest.class);

   public void testFailOnCreatePredefinedQueues() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      final String testAddress = "testAddress";
      
      final String queueName1 = "queue1";
      
      final String queueName2 = "queue2";
      
      final String queueName3 = "queue3";
      
      QueueConfiguration queue1 = new QueueConfiguration(testAddress, queueName1, null, true);
      
      QueueConfiguration queue2 = new QueueConfiguration(testAddress, queueName2, null, true);
      
      QueueConfiguration queue3 = new QueueConfiguration(testAddress, queueName3, null, true);
      
      List<QueueConfiguration> queueConfs = new ArrayList<QueueConfiguration>();
      
      queueConfs.add(queue1);
      queueConfs.add(queue2);
      queueConfs.add(queue3);
      
      conf.setQueueConfigurations(queueConfs);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
           
      server.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);
      
      try
      {
         session.createQueue(testAddress, queueName1, null, false);
         
         fail("Should throw exception");
      }
      catch (HornetQException me)
      {
         assertEquals(HornetQException.QUEUE_EXISTS, me.getCode());
      }
      try
      {
         session.createQueue(testAddress, queueName2, null, false);
         
         fail("Should throw exception");
      }
      catch (HornetQException me)
      {
         assertEquals(HornetQException.QUEUE_EXISTS, me.getCode());
      }
      try
      {
         session.createQueue(testAddress, queueName3, null, false);
         
         fail("Should throw exception");
      }
      catch (HornetQException me)
      {
         assertEquals(HornetQException.QUEUE_EXISTS, me.getCode());
      }
            
      session.close();
      
      sf.close();
      
      server.stop();
   }
   
   public void testDeploySameNames() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      final String testAddress = "testAddress";
      
      final String queueName1 = "queue1";
      
      final String queueName2 = "queue2";
      
      QueueConfiguration queue1 = new QueueConfiguration(testAddress, queueName1, null, true);
      
      QueueConfiguration queue2 = new QueueConfiguration(testAddress, queueName1, null, true);
      
      QueueConfiguration queue3 = new QueueConfiguration(testAddress, queueName2, null, true);
      
      List<QueueConfiguration> queueConfs = new ArrayList<QueueConfiguration>();
      
      queueConfs.add(queue1);
      queueConfs.add(queue2);
      queueConfs.add(queue3);
      
      conf.setQueueConfigurations(queueConfs);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
           
      server.start();
      
      Bindings bindings = server.getPostOffice().getBindingsForAddress(new SimpleString(testAddress));
      
      assertEquals(2, bindings.getBindings().size());
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);
            
      session.start();
      
      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);
      
      ClientConsumer consumer2 = session.createConsumer(queueName2);
      
      final int numMessages = 10;
      
      final SimpleString propKey = new SimpleString("testkey");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(false);
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(200);         
         assertNotNull(message);         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));         
         message.acknowledge();
         
         message = consumer2.receive(200);         
         assertNotNull(message);         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));         
         message.acknowledge();
      }
      
      assertNull(consumer1.receive(200));
      assertNull(consumer2.receive(200));

      session.close();
      
      sf.close();
      
      server.stop();
   }
   
   public void testDeployPreexistingQueues() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      final String testAddress = "testAddress";
      
      final String queueName1 = "queue1";
      
      final String queueName2 = "queue2";
      
      final String queueName3 = "queue3";
                 
      HornetQServer server = HornetQ.newHornetQServer(conf);
           
      server.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);
      
      session.createQueue(testAddress, queueName1, null, true);
        
      session.createQueue(testAddress, queueName2, null, true);
         
      session.createQueue(testAddress, queueName3, null, true);
      
      session.close();
      
      sf.close();
      
      server.stop();
      
      QueueConfiguration queue1 = new QueueConfiguration(testAddress, queueName1, null, true);
      
      QueueConfiguration queue2 = new QueueConfiguration(testAddress, queueName2, null, true);
      
      QueueConfiguration queue3 = new QueueConfiguration(testAddress, queueName3, null, true);
      
      List<QueueConfiguration> queueConfs = new ArrayList<QueueConfiguration>();
      
      queueConfs.add(queue1);
      queueConfs.add(queue2);
      queueConfs.add(queue3);
      
      conf.setQueueConfigurations(queueConfs);
      
      server.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      session = sf.createSession(false, true, true);
      
      session.start();
      
      ClientProducer producer = session.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session.createConsumer(queueName1);
      
      ClientConsumer consumer2 = session.createConsumer(queueName2);
      
      ClientConsumer consumer3 = session.createConsumer(queueName3);
      
      final int numMessages = 10;
      
      final SimpleString propKey = new SimpleString("testkey");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(false);
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(200);         
         assertNotNull(message);         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));         
         message.acknowledge();
         
         message = consumer2.receive(200);         
         assertNotNull(message);         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));         
         message.acknowledge();
         
         message = consumer3.receive(200);         
         assertNotNull(message);         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));         
         message.acknowledge();
      }
      
      assertNull(consumer1.receive(200));
      assertNull(consumer2.receive(200));
      assertNull(consumer3.receive(200));
      
      session.close();
      
      sf.close();
      
      server.stop();
   }
   
   public void testDurableNonDurable() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      final String testAddress = "testAddress";
      
      final String queueName1 = "queue1";
      
      final String queueName2 = "queue2";
      
      QueueConfiguration queue1 = new QueueConfiguration(testAddress, queueName1, null, false);
      
      QueueConfiguration queue2 = new QueueConfiguration(testAddress, queueName2, null, true);
      
      List<QueueConfiguration> queueConfs = new ArrayList<QueueConfiguration>();
      
      queueConfs.add(queue1);
      queueConfs.add(queue2);
      
      conf.setQueueConfigurations(queueConfs);
      
      HornetQServer server = HornetQ.newHornetQServer(conf);
           
      server.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);
      
      ClientProducer producer = session.createProducer(new SimpleString(testAddress));
      
      final SimpleString propKey = new SimpleString("testkey");
      
      final int numMessages = 1;
            
      log.info("sending messages");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
      
      session.close();
      
      log.info("stopping");
      
      sf.close();
      
      server.stop();
      
      server.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      session = sf.createSession(false, true, true);
      
      session.start();

      ClientConsumer consumer1 = session.createConsumer(queueName1);
      
      ClientConsumer consumer2 = session.createConsumer(queueName2);
      
      ClientMessage message = consumer1.receive(200);  
      
      assertNull(message);
            
      for (int i = 0; i < numMessages; i++)
      {
         message = consumer2.receive(200);         
         assertNotNull(message);         
         assertEquals((Integer)i, (Integer)message.getProperty(propKey));         
         message.acknowledge();
      }
      
      assertNull(consumer1.receive(200));
      assertNull(consumer2.receive(200));

      session.close();
      
      sf.close();
      
      server.stop();
   }
   
   
   public void testDeployWithFilter() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      final String testAddress = "testAddress";
      
      final String queueName1 = "queue1";
      
      final String filter = "cheese='camembert'";
      
      QueueConfiguration queue1 = new QueueConfiguration(testAddress, queueName1, filter, false);
      
      List<QueueConfiguration> queueConfs = new ArrayList<QueueConfiguration>();
      
      queueConfs.add(queue1);

      conf.setQueueConfigurations(queueConfs);
      
      HornetQServer server = HornetQ.newHornetQServer(conf, false);
           
      server.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);
      
      ClientProducer producer = session.createProducer(new SimpleString(testAddress));
      
      final SimpleString propKey = new SimpleString("testkey");
      
      final int numMessages = 1;
            
      log.info("sending messages");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);
         
         message.putStringProperty(new SimpleString("cheese"), new SimpleString("camembert"));
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
            
      session.start();

      ClientConsumer consumer1 = session.createConsumer(queueName1);
      
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
         ClientMessage message = session.createClientMessage(true);
         
         message.putStringProperty(new SimpleString("cheese"), new SimpleString("roquefort"));
         
         message.putIntProperty(propKey, i);
         
         producer.send(message);
      }
      
      assertNull(consumer1.receive(200));
            
      session.close();
      
      sf.close();
      
      server.stop();
   }
  
   
}
