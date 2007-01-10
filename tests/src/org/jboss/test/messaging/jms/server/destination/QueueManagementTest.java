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
package org.jboss.test.messaging.jms.server.destination;

import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.server.messagecounter.MessageCounter;
import org.jboss.jms.server.messagecounter.MessageStatistics;
import org.jboss.test.messaging.jms.server.destination.base.DestinationManagementTestBase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * Tests a queue's management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueManagementTest extends DestinationManagementTestBase
{
   // Constants -----------------------------------------------------
   private static final String MESSAGE_TWO = "message two";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testReloadQueue() throws Exception
   {
      String config =
         "<mbean code=\"org.jboss.jms.server.destination.QueueService\" " +
         "       name=\"somedomain:service=Queue,name=ReloadQueue\"" +
         "       xmbean-dd=\"xmdesc/Queue-xmbean.xml\">" +
         "    <depends optional-attribute-name=\"ServerPeer\">jboss.messaging:service=ServerPeer</depends>" +
         "</mbean>";

      ObjectName destObjectName = deploy(config);

      assertEquals("ReloadQueue", ServerManagement.getAttribute(destObjectName, "Name"));

      String jndiName = "/queue/ReloadQueue";
      String s = (String)ServerManagement.getAttribute(destObjectName, "JNDIName");
      assertEquals(jndiName, s);

      //Send some messages

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      Queue queue = (Queue)ic.lookup("/queue/ReloadQueue");

      Connection conn = cf.createConnection();

      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = sess.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);

      for (int i = 0; i < 10; i++)
      {
         TextMessage tm = sess.createTextMessage();

         tm.setText("message:" + i);

         prod.send(tm);
      }

      conn.close();

      //Receive half of them

      conn = cf.createConnection();

      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = sess.createConsumer(queue);

      conn.start();

      for (int i = 0; i < 5; i++)
      {
         TextMessage tm = (TextMessage)cons.receive(1000);

         assertNotNull(tm);

         assertEquals("message:" + i, tm.getText());
      }

      conn.close();

      //Undeploy and redeploy the queue
      //The last 5 persistent messages should still be there

      undeployDestination("ReloadQueue");

      deploy(config);

      queue = (Queue)ic.lookup("/queue/ReloadQueue");

      conn = cf.createConnection();

      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      cons = sess.createConsumer(queue);

      conn.start();

      for (int i = 5; i < 10; i++)
      {
         TextMessage tm = (TextMessage)cons.receive(1000);

         assertNotNull(tm);

         assertEquals("message:" + i, tm.getText());
      }

      conn.close();

      undeployDestination("ReloadQueue");
   }

   public void testMessageCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("QueueMessageCount");

      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueMessageCount");

         // Test MessageCount, should be 0 msg
         ObjectName destObjectName =
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCount");
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());

         // Send 1 message to queue
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage m = session.createTextMessage("message one");
         prod.send(m);
         conn.close();

         // Test MessageCount again, should be 1 msg
         count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(1, count.intValue());


         // Consume the message
         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();

         cons.receive();
         conn.close();

         //Need to pause for a bit since the message is not necessarily removed
         //in memory until sometime after receive has completed
         Thread.sleep(1000);

         // Test MessageCount again, should be 0 msg
         count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());
      }
      finally
      {

         ServerManagement.undeployQueue("QueueMessageCount");
      }
   }
   
   public void testScheduledMessageCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("QueueMessageCount");

      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueMessageCount");

         // Test MessageCount, should be 0 msg
         ObjectName destObjectName =
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCount");
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "ScheduledMessageCount");
         assertEquals(0, count.intValue());

         // Send 1 message to queue
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);         
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage m = session.createTextMessage("message one");
         m.setLongProperty(JBossMessage.JMS_JBOSS_SCHEDULED_DELIVERY_PROP_NAME, System.currentTimeMillis() + 1000);
         prod.send(m);
         conn.close();

         // Test MessageCount again, should be 1 msg
         count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(1, count.intValue());
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "ScheduledMessageCount");
         assertEquals(1, count.intValue());
         
         Thread.sleep(2000);
         
         count = (Integer)ServerManagement.getAttribute(destObjectName, "ScheduledMessageCount");
         assertEquals(0, count.intValue());

         // Consume the message
         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();

         cons.receive();
         conn.close();

         //Need to pause for a bit since the message is not necessarily removed
         //in memory until sometime after receive has completed
         Thread.sleep(1000);

         // Test MessageCount again, should be 0 msg
         count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());
      }
      finally
      {

         ServerManagement.undeployQueue("QueueMessageCount");
      }
   }

   public void testMessageCountOverFullSize() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Connection conn = null;

      int fullSize = 10;
      int MESSAGE_COUNT = 100;

      ServerManagement.deployQueue("QueueMessageCount2", fullSize, fullSize / 2, fullSize / 2 - 1);

      ObjectName destObjectName =
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCount2");

      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueMessageCount2");

         conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         // Send all messages to the queue and check messageCount after each send

         for(int i = 0; i < MESSAGE_COUNT; i++)
         {
            TextMessage m = session.createTextMessage("message" + i);
            prod.send(m);

            int mc = ((Integer)ServerManagement.
               getAttribute(destObjectName, "MessageCount")).intValue();

            assertEquals(i + 1, mc);
         }

         // receive messages from queue one by one and check messageCount after each receive

         MessageConsumer cons = session.createConsumer(queue);
         conn.start();

         int receivedCount = 0;

         while((cons.receive(2000)) != null)
         {
            receivedCount++;

            Thread.sleep(500);

            int mc = ((Integer)ServerManagement.
               getAttribute(destObjectName, "MessageCount")).intValue();


             if ((MESSAGE_COUNT - receivedCount)!=mc)
             {
                 retryForLogs(mc, receivedCount, MESSAGE_COUNT, destObjectName);
             }

            assertEquals(MESSAGE_COUNT - receivedCount, mc);
         }

         assertEquals(MESSAGE_COUNT, receivedCount);

      }
      finally
      {
         ServerManagement.undeployQueue("QueueMessageCount2");

         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testMaxSize() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("QueueMaxSize");

      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueMaxSize");
         
         ObjectName destObjectName =
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMaxSize");
         
         Integer i = (Integer)ServerManagement.getAttribute(destObjectName, "MaxSize");
         
         assertEquals(-1, i.intValue());
         
         ServerManagement.setAttribute(destObjectName, "MaxSize", String.valueOf(2));
         
         i = (Integer)ServerManagement.getAttribute(destObjectName, "MaxSize");
         
         assertEquals(2, i.intValue());
         
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage m1 = session.createTextMessage("message one");
         prod.send(m1);
         TextMessage m2 = session.createTextMessage("message two");
         prod.send(m2);
         TextMessage m3 = session.createTextMessage("message three");
         try
         {
            prod.send(m3);
         }
         catch (JMSException e)
         {
            //OK
         }
         
         conn.close();

         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();

         TextMessage rm1 = (TextMessage)cons.receive(1000);
         assertNotNull(rm1);
         assertEquals("message one", rm1.getText());
         
         TextMessage rm2 = (TextMessage)cons.receive(1000);
         assertNotNull(rm2);
         assertEquals("message two", rm2.getText());
         
         Message m = cons.receive(1000);
         assertNull(m);
         
         conn.close();
      }
      finally
      {
         ServerManagement.undeployQueue("QueueMaxSize");
      }
   }

   public void testRemoveAllMessages() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("QueueRemoveMessages");

      try
      {
         Queue queue = (Queue)ic.lookup("/queue/QueueRemoveMessages");

         // Send 1 message to queue
         Connection conn = cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(queue);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage m = session.createTextMessage("message one");
         prod.send(m);

         // Remove all messages from the queue
         ObjectName destObjectName =
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueRemoveMessages");
         ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);

         // Test MessageCount again, should be 0 msg
         Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
         assertEquals(0, count.intValue());

         // Send another message
         m = session.createTextMessage(MESSAGE_TWO);
         prod.send(m);
         conn.close();

         // Consume the 2nd message
         conn = cf.createConnection();
         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = session.createConsumer(queue);
         conn.start();

         Object ms = cons.receive();
         assertTrue(ms instanceof TextMessage);
         assertEquals(((TextMessage)ms).getText(), MESSAGE_TWO);
         Thread.sleep(500);
         assertNull(cons.receiveNoWait());
         conn.close();
      }
      finally
      {
         ServerManagement.undeployQueue("QueueRemoveMessages");
      }
   }

   public void testListMessages() throws Exception
   {   
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
 
      ServerManagement.deployQueue("QueueListMessages");
      
      Connection conn = null;
      
      Queue queue = (Queue)ic.lookup("/queue/QueueListMessages");      
      
      try
      {         
         conn = cf.createConnection();
   
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = s.createProducer(queue);
   
         // Send some persistent message
         TextMessage tm1 = s.createTextMessage("message1");
         tm1.setStringProperty("vegetable", "parsnip");
         TextMessage tm2 = s.createTextMessage("message2");
         tm2.setStringProperty("vegetable", "parsnip");
         TextMessage tm3 = s.createTextMessage("message3");
         tm3.setStringProperty("vegetable", "parsnip");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         
         // and some non persistent with a selector
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         TextMessage tm4 = s.createTextMessage("message4");
         tm4.setStringProperty("vegetable", "artichoke");
         TextMessage tm5 = s.createTextMessage("message5");
         tm5.setStringProperty("vegetable", "artichoke");
         TextMessage tm6 = s.createTextMessage("message6");
         tm6.setStringProperty("vegetable", "artichoke");
         prod.send(tm4);
         prod.send(tm5);
         prod.send(tm6);
         
         ObjectName destObjectName = 
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueListMessages");
          
         List allMsgs = (List)ServerManagement.invoke(destObjectName, "listAllMessages", null, null);
         
         assertNotNull(allMsgs);         
         assertEquals(6, allMsgs.size());
         
         TextMessage rm1 = (TextMessage)allMsgs.get(0);
         TextMessage rm2 = (TextMessage)allMsgs.get(1);
         TextMessage rm3 = (TextMessage)allMsgs.get(2);
         TextMessage rm4 = (TextMessage)allMsgs.get(3);
         TextMessage rm5 = (TextMessage)allMsgs.get(4);
         TextMessage rm6 = (TextMessage)allMsgs.get(5);
         
         assertEquals(tm1.getText(), rm1.getText());
         assertEquals(tm2.getText(), rm2.getText());
         assertEquals(tm3.getText(), rm3.getText());
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
         
         assertTrue(rm1.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm2.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm3.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);         
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         
         List durMsgs = (List)ServerManagement.invoke(destObjectName, "listDurableMessages", null, null);
         
         assertNotNull(durMsgs);         
         assertEquals(3, durMsgs.size());
         
         rm1 = (TextMessage)durMsgs.get(0);
         rm2 = (TextMessage)durMsgs.get(1);
         rm3 = (TextMessage)durMsgs.get(2);
                
         assertEquals(tm1.getText(), rm1.getText());
         assertEquals(tm2.getText(), rm2.getText());
         assertEquals(tm3.getText(), rm3.getText());
         
         assertTrue(rm1.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm2.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);
         assertTrue(rm3.getJMSDeliveryMode() == DeliveryMode.PERSISTENT);                 
         
         
         List nondurMsgs = (List)ServerManagement.invoke(destObjectName, "listNonDurableMessages", null, null);
         
         assertNotNull(nondurMsgs);         
         assertEquals(3, nondurMsgs.size());
               
         rm4 = (TextMessage)nondurMsgs.get(0);
         rm5 = (TextMessage)nondurMsgs.get(1);
         rm6 = (TextMessage)nondurMsgs.get(2);
                 
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
             
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         
         //Now with a selector
         
         String sel = "vegetable='artichoke'";
         
         allMsgs = (List)ServerManagement.invoke(destObjectName, "listAllMessages", new Object[] { sel }, new String[] { "java.lang.String" });
         
         assertNotNull(allMsgs);         
         assertEquals(3, allMsgs.size());
         
         rm4 = (TextMessage)allMsgs.get(0);
         rm5 = (TextMessage)allMsgs.get(1);
         rm6 = (TextMessage)allMsgs.get(2);
         
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
            
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
                  
         durMsgs = (List)ServerManagement.invoke(destObjectName, "listDurableMessages", new Object[] { sel }, new String[] { "java.lang.String" });
         
         assertNotNull(durMsgs);         
         assertEquals(0, durMsgs.size());
         
         
         nondurMsgs = (List)ServerManagement.invoke(destObjectName, "listNonDurableMessages", new Object[] { sel }, new String[] { "java.lang.String" });
         
         assertNotNull(nondurMsgs);         
         assertEquals(3, nondurMsgs.size());
               
         rm4 = (TextMessage)nondurMsgs.get(0);
         rm5 = (TextMessage)nondurMsgs.get(1);
         rm6 = (TextMessage)nondurMsgs.get(2);
                 
         assertEquals(tm4.getText(), rm4.getText());
         assertEquals(tm5.getText(), rm5.getText());
         assertEquals(tm6.getText(), rm6.getText());
             
         assertTrue(rm4.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm5.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);
         assertTrue(rm6.getJMSDeliveryMode() == DeliveryMode.NON_PERSISTENT);                  
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         
         drainDestination(cf, queue);
         
         ServerManagement.undeployQueue("QueueListMessages");
      
      }
   }

   /**
    * The jmx-console has the habit of sending an empty string if no argument is specified, so
    * we test this eventuality.
    */
   public void testListMessagesEmptySelector() throws Exception
   {
      ServerManagement.deployQueue("QueueListMessages");
      
      try
      {
   
         ObjectName destObjectName =
            new ObjectName("jboss.messaging.destination:service=Queue,name=QueueListMessages");
   
         List list = (List)ServerManagement.invoke(destObjectName,
                                                   "listAllMessages",
                                                   new Object[] {""},
                                                   new String[] {"java.lang.String"});
         assertNotNull(list);
         assertEquals(0, list.size());
   
         list = (List)ServerManagement.invoke(destObjectName,
                                                   "listAllMessages",
                                                   new Object[] {"             "},
                                                   new String[] {"java.lang.String"});
         assertNotNull(list);
         assertEquals(0, list.size());
      }
      finally
      {
         ServerManagement.undeployQueue("QueueListMessages");
      }
   }
   
   public void testMessageCounter() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "QueueStatsSamplePeriod", String.valueOf(1000));
      

      ServerManagement.deployQueue("QueueMessageCounter");
      
      Queue queue = (Queue)ic.lookup("/queue/QueueMessageCounter");      

      ObjectName destObjectName =
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCounter");
      
      //Most of this is tested in ServerPeerTest
      
      MessageCounter counter = (MessageCounter)ServerManagement.getAttribute(destObjectName, "MessageCounter");
      
      assertNotNull(counter);
      
      assertEquals(0, counter.getCount());
      
      assertEquals(0, counter.getCountDelta());
      
      assertEquals(-1, counter.getHistoryLimit());
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
      
      TextMessage tm1 = sess.createTextMessage("message1");
      prod.send(tm1);
      
      TextMessage tm2 = sess.createTextMessage("message2");
      prod.send(tm2);
      
      TextMessage tm3 = sess.createTextMessage("message3");
      prod.send(tm3);
      
      Thread.sleep(1500);
      
      assertEquals(3, counter.getCount());
      
      assertEquals(3, counter.getCountDelta());
      
      assertEquals(3, counter.getCount());
      
      assertEquals(0, counter.getCountDelta());
      
      MessageStatistics stats = (MessageStatistics)ServerManagement.getAttribute(destObjectName, "MessageStatistics");
      
      assertNotNull(stats);
      
      assertEquals(3, stats.getCount());
      
      assertEquals(0, stats.getCountDelta());
      
      assertEquals(3, stats.getDepth());
      
      ServerManagement.invoke(destObjectName, "resetMessageCounter", null, null);
      
      assertEquals(0, counter.getCount());
      
      ServerManagement.invoke(destObjectName, "resetMessageCounterHistory", null, null);
      
      String html = (String)ServerManagement.invoke(destObjectName, "listMessageCounterHistoryAsHTML", null, null);
      
      assertNotNull(html);
      
      html = (String)ServerManagement.invoke(destObjectName, "listMessageCounterAsHTML", null, null);
      
      assertNotNull(html);
      
      assertTrue(html.indexOf("QueueMessageCounter") != -1);
            
      
      ServerManagement.undeployQueue("QueueMessageCounter");

   }
   
   public void testConsumersCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      

      ServerManagement.deployQueue("QueueConsumerCount");
      
      Queue queue = (Queue)ic.lookup("/queue/QueueConsumerCount");      

      ObjectName destObjectName =
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueConsumerCount");
          
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Integer i = (Integer)ServerManagement.getAttribute(destObjectName, "ConsumerCount");
      
      assertEquals(0, i.intValue());
      
      MessageConsumer cons1 = sess.createConsumer(queue);
      
      MessageConsumer cons2 = sess.createConsumer(queue);
      
      MessageConsumer cons3 = sess.createConsumer(queue);
      
      i = (Integer)ServerManagement.getAttribute(destObjectName, "ConsumerCount");
      
      assertEquals(3, i.intValue());
      
      cons3.close();
      
      i = (Integer)ServerManagement.getAttribute(destObjectName, "ConsumerCount");
      
      assertEquals(2, i.intValue());
      
      cons1.close();
      
      cons2.close();
      
      i = (Integer)ServerManagement.getAttribute(destObjectName, "ConsumerCount");
      
      assertEquals(0, i.intValue());
      
      conn.close();
      
      ServerManagement.undeployQueue("QueueConsumerCount");

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return true;
   }

   // Private -------------------------------------------------------
   
   /** this method will retry getting MessageCount until it gets a sucessful result.
    *  This is done just so you can visualize a logs what's the real racing condition. (caused by the delivery thread at the time of the construction of this method)
    */
   private void retryForLogs(int mc, int receivedCount, int MESSAGE_COUNT, ObjectName destObjectName) throws Exception
   {
      int retry=0;
      while ((MESSAGE_COUNT - receivedCount != mc) && retry < 10)
      {
         log.info("******************** Still failing");
         mc = ((Integer) ServerManagement.
                  getAttribute(destObjectName, "MessageCount")).intValue();
         Thread.sleep(50);
         retry++;
      }
      if (retry<10)
      {
         log.info("There is a racing condition that was fixed after " + retry + " retries. Look at log4j traces.");
      }
   }

   // Inner classes -------------------------------------------------
}
