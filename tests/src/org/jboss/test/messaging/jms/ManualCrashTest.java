/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.messaging.jms;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;


/**
 * 
 * A CrashTest.
 * 
 * For now crash tests should be run manually due to complexities in killing server
 * but leaving database intact.
 * 
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version 1.1
 *
 * ManualCrashTest.java,v 1.1 2006/03/29 18:51:23 timfox Exp
 */
public class ManualCrashTest extends MessagingTestCase
{
   
   //  Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected InitialContext initialContext;
   
   
   protected ConnectionFactory cf;
   protected Queue queue;
   protected Topic topic;
   
   // Constructors --------------------------------------------------
   
   public ManualCrashTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();                 
   }
   
   public void tearDown() throws Exception
   {
      super.tearDown();     
   }
   
   protected void doSetup() throws Exception
   {
      Hashtable env = new Hashtable();
      env.put("java.naming.factory.initial",
      "org.jnp.interfaces.NamingContextFactory");     
      env.put("java.naming.provider.url", "jnp://localhost:1099");
      env.put("java.naming.factory.url.pkg", "org.jboss.naming:org.jnp.interfaces");
      
      initialContext = new InitialContext(env);
            
      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      queue = (Queue)initialContext.lookup("/queue/testQueue");
      
      topic = (Topic)initialContext.lookup("/topic/openTopic");
      
      initialContext.close();
   }
   
   // Public --------------------------------------------------------
 
 
   public void testNonTxQueue() throws Exception
   {            
      doSetup();
      
      this.drainDestination(cf, queue);
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
          
      int i = 0;
      
      //Thread.sleep(30000);
      
      try
      {         
         log.info("Please manually kill server process in a few seconds time...");
         while (true)
         {
            TextMessage tm = sess.createTextMessage("message:" + i);
            
            tm.setIntProperty("message", i);
            
            prod.send(tm);
            
            i++;
         }         
      }
      catch (Exception e)
      {
         //ok
         log.info("Caught exception ok");
      }
      
      log.info("The last message sent was " + i);
      
      log.info("Now restart the server.... you have 30 seconds to comply");
      
      Thread.sleep(30000);
      
      doSetup();
      
      conn = cf.createConnection();
      
      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons = sess.createConsumer(queue);
      
      conn.start();
      
      //Consume messages and make sure we get the right ones
      
      int count = 0;
      
      while (true)
      {
         TextMessage m = (TextMessage)cons.receive(1000);
         
         assertNotNull(m);
         
         int c = m.getIntProperty("message");
         
         assertEquals(count, c);
         
         log.info("Received message:" + count);
         
         count++;
         
         if (count == i)
         {
            //Next message should be null
            Message m2 = cons.receive(1000);
            
            assertNull(m2);
            
            break;
         }         
      }
      
      conn.close();
   }
//   
//   public void testNonTxDurableSubs() throws Exception
//   {            
//      doSetup();
//      
//      this.drainSub(cf, topic, "sub1", "clientid1");
//      this.drainSub(cf, topic, "sub2", "clientid1");
//      this.drainSub(cf, topic, "sub3", "clientid1");
//      this.drainSub(cf, topic, "sub4", "clientid1");
//      this.drainSub(cf, topic, "sub5", "clientid1");
//      
//      Connection conn = cf.createConnection();
//      conn.setClientID("clientid1");
//            
//      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      
//      MessageConsumer sub1 = sess.createDurableSubscriber(topic, "sub1");
//      MessageConsumer sub2 = sess.createDurableSubscriber(topic, "sub2");
//      MessageConsumer sub3 = sess.createDurableSubscriber(topic, "sub3");
//      MessageConsumer sub4 = sess.createDurableSubscriber(topic, "sub4");
//      MessageConsumer sub5 = sess.createDurableSubscriber(topic, "sub5");
//         
//      
//      MessageProducer prod = sess.createProducer(topic);
//          
//      int i = 0;
//      
//      //Thread.sleep(30000);
//      
//      try
//      {         
//         log.info("Please manually kill server process in a few seconds time...");
//         while (true)
//         {
//            TextMessage tm = sess.createTextMessage("message:" + i);
//            
//            tm.setIntProperty("message", i);
//            
//            prod.send(tm);
//            
//            i++;
//         }         
//      }
//      catch (Exception e)
//      {
//         //ok
//         log.info("Caught exception ok");
//      }
//      
//      log.info("The last message sent was " + i);
//      
//      log.info("Now restart the server.... you have 30 seconds to comply");
//      
//      //any messages sent should always be sent to ALL subs never just half of them, say
//      
//      Thread.sleep(30000);
//      
//      doSetup();
//      
//      conn = cf.createConnection();
//      
//      conn.setClientID("clientid1");
//      
//      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      
//      sub1 = sess.createDurableSubscriber(topic, "sub1");
//      sub2 = sess.createDurableSubscriber(topic, "sub2");
//      sub3 = sess.createDurableSubscriber(topic, "sub3");
//      sub4 = sess.createDurableSubscriber(topic, "sub4");
//      sub5 = sess.createDurableSubscriber(topic, "sub5");
//         
//      conn.start();
//      
//      //Consume messages and make sure we get the right ones
//      
//      int count = 0;
//      
//      while (true)
//      {
//         TextMessage m1 = (TextMessage)sub1.receive(1000);
//         TextMessage m2 = (TextMessage)sub2.receive(1000);
//         TextMessage m3 = (TextMessage)sub3.receive(1000);
//         TextMessage m4 = (TextMessage)sub4.receive(1000);
//         TextMessage m5 = (TextMessage)sub5.receive(1000);
//         
//         assertNotNull(m1);
//         assertNotNull(m2);
//         assertNotNull(m3);
//         assertNotNull(m4);
//         assertNotNull(m5);
//         
//         int c = m1.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m2.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m3.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m4.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m5.getIntProperty("message");         
//         assertEquals(count, c);
//         
//         log.info("Received message:" + count);
//         
//         count++;
//         
//         if (count == i)
//         {
//            //Next message should be null
//            Message m2_1 = sub1.receive(1000);            
//            assertNull(m2_1);
//            Message m2_2 = sub2.receive(1000);            
//            assertNull(m2_2);
//            Message m2_3 = sub3.receive(1000);            
//            assertNull(m2_3);
//            Message m2_4 = sub4.receive(1000);            
//            assertNull(m2_4);
//            Message m2_5 = sub5.receive(1000);            
//            assertNull(m2_5);
//            
//            break;
//         }         
//      }
//      
//      conn.close();
//   }
//   
//   public void testTxQueue() throws Exception
//   {            
//      doSetup();
//      
//      this.drainDestination(cf, queue);
//      
//      Connection conn = cf.createConnection();
//      
//      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
//      
//      MessageProducer prod = sess.createProducer(queue);
//          
//      int transactions = 0;
//      
//      final int TRANSACTION_SIZE = 50;
//      
//      int count = 0;
//      
//      try
//      {         
//         log.info("Please manually kill server process in a few seconds time...");
//         while (true)
//         {
//            TextMessage tm = sess.createTextMessage("message:" + count);
//            
//            tm.setIntProperty("message", count);
//            
//            prod.send(tm);
//            
//            count++;
//            
//            if (count % TRANSACTION_SIZE == 0)
//            {
//               sess.commit();
//               transactions++;
//            }
//         }         
//      }
//      catch (Exception e)
//      {
//         //ok
//         log.info("Caught exception ok");
//      }
//      
//      log.info("Number of transactions sent: " + transactions);
//      
//      log.info("Now restart the server.... you have 30 seconds to comply");
//      
//      Thread.sleep(30000);
//      
//      doSetup();
//      
//      conn = cf.createConnection();
//      
//      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      
//      MessageConsumer cons = sess.createConsumer(queue);
//      
//      conn.start();
//      
//      //Consume messages and make sure we get the right ones
//      
//      int expectedMessages = transactions * TRANSACTION_SIZE;
//      
//      count = 0;
//      
//      while (true)
//      {
//         TextMessage m = (TextMessage)cons.receive(1000);
//         
//         assertNotNull(m);
//         
//         int c = m.getIntProperty("message");
//         
//         assertEquals(count, c);
//         
//         log.info("Received message:" + count);
//         
//         count++;
//         
//         if (count == expectedMessages)
//         {
//            //Next message should be null
//            Message m2 = cons.receive(1000);
//            
//            assertNull(m2);
//            
//            break;
//         }         
//      }
//      
//      conn.close();
//   }
//   
//   
//   public void testTxDurableSubs() throws Exception
//   {            
//      doSetup();
//      
//      this.drainSub(cf, topic, "sub1", "clientid1");
//      this.drainSub(cf, topic, "sub2", "clientid1");
//      this.drainSub(cf, topic, "sub3", "clientid1");
//      this.drainSub(cf, topic, "sub4", "clientid1");
//      this.drainSub(cf, topic, "sub5", "clientid1");
//      
//      Connection conn = cf.createConnection();
//      conn.setClientID("clientid1");
//            
//      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
//      
//      MessageConsumer sub1 = sess.createDurableSubscriber(topic, "sub1");
//      MessageConsumer sub2 = sess.createDurableSubscriber(topic, "sub2");
//      MessageConsumer sub3 = sess.createDurableSubscriber(topic, "sub3");
//      MessageConsumer sub4 = sess.createDurableSubscriber(topic, "sub4");
//      MessageConsumer sub5 = sess.createDurableSubscriber(topic, "sub5");
//         
//      
//      MessageProducer prod = sess.createProducer(topic);
//          
//      int transactions = 0;
//      
//      final int TRANSACTION_SIZE = 50;
//      
//      int count = 0;
//      
//      try
//      {         
//         log.info("Please manually kill server process in a few seconds time...");
//         while (true)
//         {
//            TextMessage tm = sess.createTextMessage("message:" + count);
//            
//            tm.setIntProperty("message", count);
//            
//            prod.send(tm);
//            
//            count++;
//            
//            if (count % TRANSACTION_SIZE == 0)
//            {
//               sess.commit();
//               
//               transactions++;
//            }
//         }         
//      }
//      catch (Exception e)
//      {
//         //ok
//         e.printStackTrace();
//         log.info("Caught exception ok");
//      }
//      
//      log.info("Number of transactions sent: " + transactions);
//      
//      log.info("Now restart the server.... you have 30 seconds to comply");
//      
//      //any messages sent should always be sent to ALL subs never just half of them, say
//      
//      Thread.sleep(30000);
//      
//      doSetup();
//      
//      conn = cf.createConnection();
//      
//      conn.setClientID("clientid1");
//      
//      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      
//      sub1 = sess.createDurableSubscriber(topic, "sub1");
//      sub2 = sess.createDurableSubscriber(topic, "sub2");
//      sub3 = sess.createDurableSubscriber(topic, "sub3");
//      sub4 = sess.createDurableSubscriber(topic, "sub4");
//      sub5 = sess.createDurableSubscriber(topic, "sub5");
//         
//      conn.start();
//      
//      //Consume messages and make sure we get the right ones
//      
//      int expectedMessages = transactions * TRANSACTION_SIZE;
//      
//      count = 0;
//      
//      while (true)
//      {
//         TextMessage m1 = (TextMessage)sub1.receive(1000);
//         TextMessage m2 = (TextMessage)sub2.receive(1000);
//         TextMessage m3 = (TextMessage)sub3.receive(1000);
//         TextMessage m4 = (TextMessage)sub4.receive(1000);
//         TextMessage m5 = (TextMessage)sub5.receive(1000);
//         
//         assertNotNull(m1);
//         assertNotNull(m2);
//         assertNotNull(m3);
//         assertNotNull(m4);
//         assertNotNull(m5);
//         
//         int c = m1.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m2.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m3.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m4.getIntProperty("message");         
//         assertEquals(count, c);
//         c = m5.getIntProperty("message");         
//         assertEquals(count, c);
//         
//         log.info("Received message:" + count);
//         
//         count++;
//         
//         if (count == expectedMessages)
//         {
//            //Next message should be null
//            Message m2_1 = sub1.receive(1000);            
//            assertNull(m2_1);
//            Message m2_2 = sub2.receive(1000);            
//            assertNull(m2_2);
//            Message m2_3 = sub3.receive(1000);            
//            assertNull(m2_3);
//            Message m2_4 = sub4.receive(1000);            
//            assertNull(m2_4);
//            Message m2_5 = sub5.receive(1000);            
//            assertNull(m2_5);
//            
//            break;
//         }         
//      }
//      
//      conn.close();
//   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}


