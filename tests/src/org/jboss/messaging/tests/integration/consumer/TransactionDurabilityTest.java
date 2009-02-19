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
package org.jboss.messaging.tests.integration.consumer;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.SimpleString;
/**
 * 
 * A TransactionDurabilityTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 16 Jan 2009 11:00:33
 *
 *
 */
public class TransactionDurabilityTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(TransactionDurabilityTest.class);

   /*
    * This tests the following situation:
    * 
    * (With the old implementation)
    * Currently when a new persistent message is routed to persistent queues, the message is first stored, then the message is routed.
    * Let's say it has been routed to two different queues A, B.
    * Ref R1 gets consumed and acknowledged by transacted session S1, this decrements the ref count and causes an acknowledge record to be written to storage,
    * transactionally, but it's not committed yet.
    * Ref R2 then gets consumed and acknowledged by non transacted session S2, this causes a delete record to be written to storage.
    * R1 then rolls back, and the server is restarted - unfortunatelt since the delete record was written R1 is not ready to be consumed again.
    * 
    * It's therefore crucial the messages aren't deleted from storage until AFTER any ack records are committed to storage.
    * 
    * 
    */
   public void testRolledBackAcknowledgeWithSameMessageAckedByOtherSession() throws Exception
   {
      Configuration conf = createDefaultConfig();
      
      final SimpleString testAddress = new SimpleString("testAddress");
      
      final SimpleString queue1 = new SimpleString("queue1");
      
      final SimpleString queue2 = new SimpleString("queue2");
                   
      MessagingService messagingService = createService(true, conf); 
      
      messagingService.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session1 = sf.createSession(false, true, true);
      
      ClientSession session2 = sf.createSession(false, false, false);

      session1.createQueue(testAddress, queue1, null, true, false);
      
      session1.createQueue(testAddress, queue2, null, true, false);

      ClientProducer producer = session1.createProducer(testAddress);

      ClientMessage message = session1.createClientMessage(true);
         
      producer.send(message);
      
      session1.start();
      
      session2.start();
                  
      ClientConsumer consumer1 = session1.createConsumer(queue1);
      
      ClientConsumer consumer2 = session2.createConsumer(queue2);
      
      ClientMessage m1 = consumer1.receive(1000);
      
      assertNotNull(m1);
      
      ClientMessage m2 = consumer2.receive(1000);
      
      assertNotNull(m2);
      
      m2.acknowledge();
      
      //Don't commit session 2
      
      m1.acknowledge();
      
      session2.rollback();
      
      session1.close();
      
      session2.close();
      
      messagingService.stop();
      
      messagingService.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      session1 = sf.createSession(false, true, true);
      
      session2 = sf.createSession(false, true, true);
      
      session1.start();
      
      session2.start();
      
      consumer1 = session1.createConsumer(queue1);
      
      consumer2 = session2.createConsumer(queue2);
      
      m1 = consumer1.receive(100);
      
      assertNull(m1);
      
      m2 = consumer2.receive(1000);
      
      assertNotNull(m2);
      
      m2.acknowledge();
      
      session1.close();
      
      session2.close();
      
      messagingService.stop();
      
      messagingService.start();
      
      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      session1 = sf.createSession(false, true, true);
      
      session2 = sf.createSession(false, true, true);
      
      session1.start();
      
      session2.start();
      
      consumer1 = session1.createConsumer(queue1);
      
      consumer2 = session2.createConsumer(queue2);
      
      m1 = consumer1.receive(100);
      
      assertNull(m1);
      
      m2 = consumer2.receive(100);
      
      assertNull(m2);
      
      session1.close();
      
      session2.close();
      
      messagingService.stop();
      
   }

}

