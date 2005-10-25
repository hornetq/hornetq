/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.server.ClientManager;
import org.jboss.jms.server.ServerPeer;
import org.jboss.jms.server.endpoint.ServerConnectionDelegate;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 *
 */
public class ConnectionCleanupTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;
   protected Topic topic;

   // Constructors --------------------------------------------------

   public ConnectionCleanupTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.init("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      queue = (Destination)initialContext.lookup("/queue/Queue"); 
      topic = (Topic)initialContext.lookup("/topic/Topic");      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deInit();
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
   public void testNoPing() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      ServerPeer sp = ServerManagement.getServerPeer();
      ClientManager cm = sp.getClientManager();
      cm.setConnectionCloseTime(8000);
      cm.setPingCheckInterval(3000);
      
      JBossConnection conn = (JBossConnection)cf.createConnection();
      
      Serializable connectionID = conn.getConnectionID();
      
      ServerConnectionDelegate scd = cm.getConnectionDelegate(connectionID);
      assertNotNull(scd);
      assertEquals(connectionID, scd.getConnectionID());
      
      //disable the pinger
      conn.getPinger().stop();
      
      //wait for enough time for the server to clean-up the connection
      Thread.sleep(15000);
      
      scd = cm.getConnectionDelegate(connectionID);
      assertNull(scd);
      
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}

