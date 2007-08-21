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
package org.jboss.test.messaging;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.message.MessageIdGeneratorFactory;
import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.util.ProxyAssertSupport;
import org.jboss.tm.TransactionManagerService;

/**
 * The base case for messaging tests.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class MessagingTestCase extends ProxyAssertSupport 
{
   // Constants -----------------------------------------------------

   public final static int MAX_TIMEOUT = 1000 * 10 /* seconds */;

   public final static int MIN_TIMEOUT = 1000 * 1 /* seconds */;
   

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   public MessagingTestCase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      String banner =
         "####################################################### Start " +
         (isRemote() ? "REMOTE" : "IN-VM") + " test: " + getName();

      log.info(banner);

      if (isRemote())
      {
         // log the test start in the remote log, this will make hunting through logs so much easier
         ServerManagement.log(ServerManagement.INFO, banner);
      }
      
      MessageIdGeneratorFactory.instance.clear();
   }

   protected void tearDown() throws Exception
   {
      String banner =
         "####################################################### Stop " + 
         (isRemote() ? "REMOTE" : "IN-VM") + " test: " + getName();

      log.info(banner);
      
      if (isRemote())
      {
         // log the test stop in the remote log, this will make hunting through logs so much easier
         ServerManagement.log(ServerManagement.INFO, banner);
      }
   }
      
   protected void removeAllMessages(String destName, boolean isQueue, int server) throws Exception
   {
   	String on = "jboss.messaging.destination:service=" + (isQueue ? "Queue" : "Topic") + ",name=" + destName;
   	
   	ServerManagement.getServer(server).invoke(new ObjectName(on), "removeAllMessages", null, null);
   }
   
   protected void checkEmpty(Queue queue) throws Exception
   {
   	ObjectName destObjectName =  new ObjectName("jboss.messaging.destination:service=Queue,name=" + queue.getQueueName());
   	
      Integer messageCount = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
       
      if (messageCount.intValue() != 0)
      {
      	//Delete before failing - so as not to make all other tests fail
      	removeAllMessages(queue.getQueueName(), true, 0);
      	
      	fail("Message count for queue " + queue.getQueueName() + " on server is " + messageCount);
      }    
   }
   
   protected void checkEmpty(Queue queue, int server) throws Exception
   {
   	ObjectName destObjectName =  new ObjectName("jboss.messaging.destination:service=Queue,name=" + queue.getQueueName());
   	
      Integer messageCount = (Integer)ServerManagement.getServer(server).getAttribute(destObjectName, "MessageCount");
      
      assertEquals(0, messageCount.intValue());
   }
   
   protected void checkEmpty(Topic topic) throws Exception
   {
   	ObjectName destObjectName =  new ObjectName("jboss.messaging.destination:service=Topic,name=" + topic.getTopicName());
   	
      Integer messageCount = (Integer)ServerManagement.getAttribute(destObjectName, "AllMessageCount"); 
      
      assertEquals(0, messageCount.intValue());    
   }
         
   protected void checkNoSubscriptions(Topic topic) throws Exception
   {
   	ObjectName destObjectName =  new ObjectName("jboss.messaging.destination:service=Topic,name=" + topic.getTopicName());
   	
      Integer messageCount = (Integer)ServerManagement.getAttribute(destObjectName, "AllSubscriptionsCount"); 
      
      assertEquals(0, messageCount.intValue());      
   }
   
   protected void checkNoSubscriptions(Topic topic, int server) throws Exception
   {
   	ObjectName destObjectName =  new ObjectName("jboss.messaging.destination:service=Topic,name=" + topic.getTopicName());
   	
      Integer messageCount = (Integer)ServerManagement.getServer(server).getAttribute(destObjectName, "AllSubscriptionsCount"); 
      
      assertEquals(0, messageCount.intValue());      
   }
      
   protected boolean assertRemainingMessages(int expected) throws Exception
   {
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1");
      Integer messageCount = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount"); 
      
      log.trace("There are " + messageCount + " messages");
      
      assertEquals(expected, messageCount.intValue());      
      return expected == messageCount.intValue();
   }
   
   protected int getNumberOfMessages(Queue queue, int server) throws Exception
   {
   	ObjectName destObjectName =  new ObjectName("jboss.messaging.destination:service=Queue,name=" + queue.getQueueName());
   	
      Integer messageCount = (Integer)ServerManagement.getServer(server).getAttribute(destObjectName, "MessageCount");
      
      return messageCount.intValue();
   }
   
   protected void drainDestination(ConnectionFactory cf, Destination dest) throws Exception
   {
      Connection conn = null;
      try
      {         
         conn = cf.createConnection();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createConsumer(dest);
         Message m = null;
         conn.start();
         log.trace("Draining messages from " + dest);
         while (true)
         {
            m = cons.receive(500);
            if (m == null) break;
            log.trace("Drained message");
         }         
      }
      finally
      {
         if (conn!= null) conn.close();
      }
   }
   
   protected void drainSub(ConnectionFactory cf, Topic topic, String subName, String clientID) throws Exception
   {
      Connection conn = null;
      try
      {         
         conn = cf.createConnection();
         conn.setClientID(clientID);
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons = sess.createDurableSubscriber(topic, subName);
         Message m = null;
         conn.start();
         log.trace("Draining messages from " + topic + ":" + subName);
         while (true)
         {
            m = cons.receive(500);
            if (m == null) break;
            log.trace("Drained message");
         }         
      }
      finally
      {
         if (conn!= null) conn.close();
      }
   }
   
   protected boolean checkNoBindingData() throws Exception
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();
      
      java.sql.Connection conn = null;
      
      PreparedStatement ps = null;
      
      ResultSet rs = null;

      try
      {
         conn = ds.getConnection();
         String sql = "SELECT * FROM JBM_POSTOFFICE";
         ps = conn.prepareStatement(sql);
         
         rs = ps.executeQuery();
         
         return rs.next();
      }
      finally
      {
         if (rs != null) rs.close();
         
         if (ps != null) ps.close();
         
         if (conn != null) conn.close();
         
         mgr.commit();

         if (txOld != null)
         {
            mgr.resume(txOld);
         }
                  
      } 
   }
   
   protected boolean checkNoMessageData() throws Exception
   {
      //Can't do this remotely
      
      if (ServerManagement.isRemote())
      {
         return false;
      }
      
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();
      
      java.sql.Connection conn = null;
      
      PreparedStatement ps = null;
      
      ResultSet rs = null;

      try
      {
         conn = ds.getConnection();
         String sql = "SELECT * FROM JBM_MSG_REF";
         ps = conn.prepareStatement(sql);
         
         rs = ps.executeQuery();
         
         boolean exists = rs.next();
         
         if (exists)
         {
         	log.info("Message reference data exists");
         }
         
         if (!exists)
         {
            rs.close();
            
            ps.close();
            
            ps = conn.prepareStatement("SELECT * FROM JBM_MSG");
            
            rs = ps.executeQuery();
           
            exists = rs.next();      
            
            if (exists)
            {
            	log.info("Message data exists");
            }
         }
         
         return exists;
      }
      finally
      {
         if (rs != null) rs.close();
         
         if (ps != null) ps.close();
         
         if (conn != null) conn.close();
         
         mgr.commit();

         if (txOld != null)
         {
            mgr.resume(txOld);
         }
                  
      } 
   }
   
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
   
   protected List getReferenceIds(long channelId) throws Throwable
   {
      InitialContext ctx = new InitialContext();

      TransactionManager mgr = (TransactionManager)ctx.lookup(TransactionManagerService.JNDI_NAME);
      DataSource ds = (DataSource)ctx.lookup("java:/DefaultDS");
      
      javax.transaction.Transaction txOld = mgr.suspend();
      mgr.begin();

      java.sql.Connection conn = ds.getConnection();
      String sql = "SELECT MESSAGE_ID FROM JBM_MSG_REF WHERE CHANNEL_ID=? ORDER BY ORD";
      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setLong(1, channelId);
   
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
   
         

   
   
   


   /**
    * @return true if this test is ran in "remote" mode, i.e. the server side of the test runs in a
    *         different VM than this one (that is running the client side)
    */
   protected boolean isRemote()
   {
      return ServerManagement.isRemote();
   }

   /**
    * @param conn a JMS connection
    * @return the ID of the ServerPeer the connection is communicating with.
    */
   protected static int getServerId(Connection conn)
   {
      return ((JBossConnection) conn).getServerID();
   }
   
   protected Connection createConnectionOnServer(ConnectionFactory factory, int serverId)
   throws Exception
   {
   	int count=0;

   	while (true)
   	{
   		if (count++>10)
   			throw new IllegalStateException("Cannot make connection to node " + serverId);

   		Connection connection = factory.createConnection();

   		if (getServerId(connection) == serverId)
   		{
   			return connection;
   		}
   		else
   		{
   			connection.close();
   		}
   	}
   }

   protected Connection createConnectionOnServer(ConnectionFactory factory, int serverId, String user, String password)
   throws Exception
   {
   	int count=0;

   	while (true)
   	{
   		if (count++>10)
   			throw new IllegalStateException("Cannot make connection to node " + serverId);

   		Connection connection = factory.createConnection(user, password);

   		if (getServerId(connection) == serverId)
   		{
   			return connection;
   		}
   		else
   		{
   			connection.close();
   		}
   	}
   }

   protected XAConnection createXAConnectionOnServer(XAConnectionFactory factory, int serverId)
   throws Exception
   {
   	int count=0;

   	while (true)
   	{
   		if (count++>10)
   			return null;

   		XAConnection connection = factory.createXAConnection();

   		if (getServerId(connection) == serverId)
   		{
   			return connection;
   		}
   		else
   		{
   			connection.close();
   		}
   	}
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
