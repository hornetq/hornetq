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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.messaging.core.message.SimpleMessageStore;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.tm.TransactionManagerService;

/**
 * 
 * A MessageCleanupTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MessageCleanupTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext ic;
   protected ConnectionFactory cf;
   
   protected Topic topic;

   // Constructors --------------------------------------------------
   
   public MessageCleanupTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();                  
      
      ServerManagement.start("all");
      
      
      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.undeployTopic("TestTopic");
      ServerManagement.deployTopic("TestTopic", 100, 10, 10);
      
      topic = (Topic)ic.lookup("/topic/TestTopic");

      log.debug("setup done");
   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("TestTopic");
      
      super.tearDown();

      log.debug("tear down done");
   }
   
   /*
    * Test that all messages on a non durable sub are removed on close
    */
   public void testNonDurableClose() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      Connection conn = cf.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(topic);
      
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      MessageConsumer cons = sess.createConsumer(topic);
                  
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();
      
      assertEquals(100, ms.messageIds().size());
      
      //50 Should be paged onto disk
      
      assertEquals(50, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the consumer
      
      cons.close();
      
      assertEquals(0, ms.messageIds().size());
      
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());
      
      conn.close();
   }
   
   public void testNonDurableClose2() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      Connection conn = cf.createConnection();
      
      conn.setClientID("wibble12345");
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(topic);
      
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      MessageConsumer cons1 = sess.createConsumer(topic);
      
      MessageConsumer cons2 = sess.createDurableSubscriber(topic, "sub1");
                  
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();
      
      assertEquals(100, ms.messageIds().size());
      
      assertEquals(100, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the consumers
      
      cons1.close();
      cons2.close();
      
      assertEquals(100, ms.messageIds().size());
      
      assertEquals(50, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      sess.unsubscribe("sub1");
      
      assertEquals(0, ms.messageIds().size());
      
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());
      
      
      conn.close();
   }
   


   /*
    * Test that all messages on a temporary queue are removed on close
    */
   public void testTemporaryQueueClose() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      String objectName = "somedomain:service=TempQueueConnectionFactory";
      String[] jndiBindings = new String[] { "/TempQueueConnectionFactory" };

      ServerManagement.deployConnectionFactory(objectName, jndiBindings, 150, 100, 10, 10);

      ConnectionFactory cf2 = (ConnectionFactory)ic.lookup("/TempQueueConnectionFactory");
      
      Connection conn = cf2.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryQueue queue = sess.createTemporaryQueue();
      
      MessageProducer prod = sess.createProducer(queue);
      
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
           
      conn.start();
      
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();
      
      assertEquals(100, ms.messageIds().size());
      
      assertEquals(50, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the connection
      
      conn.close();
      
      assertEquals(0, ms.messageIds().size());
      
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());
      
   }
   
   /*
    * Test that all messages on a temporary topic are removed on close
    */
   public void testTemporaryTopicClose() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      String objectName = "somedomain:service=TempTopicConnectionFactory";
      String[] jndiBindings = new String[] { "/TempTopicConnectionFactory" };

      ServerManagement.deployConnectionFactory(objectName, jndiBindings, 150, 100, 10, 10);

      ConnectionFactory cf2 = (ConnectionFactory)ic.lookup("/TempTopicConnectionFactory");
      
      Connection conn = cf2.createConnection();
      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryTopic topic = sess.createTemporaryTopic();
      
      MessageProducer prod = sess.createProducer(topic);
      
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
           
      sess.createConsumer(topic);
      
      sess.createConsumer(topic);
      
      //Don't start the connection
      
      for (int i = 0; i < 150; i++)
      {
         prod.send(sess.createMessage());
      }
      
      SimpleMessageStore ms = (SimpleMessageStore)ServerManagement.getMessageStore();
      
      assertEquals(100, ms.messageIds().size());
      
      assertEquals(100, getReferenceIds().size());
      
      assertEquals(50, getMessageIds().size());
      
      //Now we close the connection
      
      conn.close();
      
      assertEquals(0, ms.messageIds().size());
      
      assertEquals(0, getReferenceIds().size());
      
      assertEquals(0, getMessageIds().size());
      
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   protected List getReferenceIds() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGE_ID, ORD FROM JBM_MSG_REF";
      PreparedStatement ps = conn.prepareStatement(sql);
   
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }
   
   
   protected List getMessageIds() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGE_ID FROM JBM_MSG ORDER BY MESSAGE_ID";
      PreparedStatement ps = conn.prepareStatement(sql);
      
      ResultSet rs = ps.executeQuery();
      
      List msgIds = new ArrayList();
      
      while (rs.next())
      {
         long msgId = rs.getLong(1);
         msgIds.add(new Long(msgId));
      }
      rs.close();
      ps.close();
      conn.close();

      mgr.commit();

      if (txOld != null)
      {
         mgr.resume(txOld);
      }
      
      return msgIds;
   }
   
   // Inner classes -------------------------------------------------
   
   
}


