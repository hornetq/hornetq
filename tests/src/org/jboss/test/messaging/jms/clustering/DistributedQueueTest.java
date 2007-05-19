/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnection;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceAttributeOverrides;

/**
 * Extending MessagingTestCase and not ClusteringTestBase because I want to start the messaging
 * servers (the clustered post offices in this case) configured in a particular way (a specific
 * message redistribution policy).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedQueueTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DistributedQueueTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testMessageRedistributionAmongNodes() throws Exception
   {
      // start servers with redistribution policies that actually do something
      ServiceAttributeOverrides attrOverrides = new ServiceAttributeOverrides();

      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");

      attrOverrides.
         put(postOfficeObjectName, "MessagePullPolicy",
             "org.jboss.messaging.core.plugin.postoffice.cluster.DefaultMessagePullPolicy");

      attrOverrides.put(postOfficeObjectName, "StatsSendPeriod", new Long(1000));

      ServerManagement.start(0, "all", attrOverrides, true);
      ServerManagement.start(1, "all", attrOverrides, false);

      ServerManagement.deployQueue("testDistributedQueue", 0);
      ServerManagement.deployQueue("testDistributedQueue", 1);

      InitialContext ic0 = null;
      InitialContext ic1 = null;
      Connection conn = null;

      try
      {
         ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));
         ic1 = new InitialContext(ServerManagement.getJNDIEnvironment(1));

         ConnectionFactory cf = (ConnectionFactory)ic0.lookup("/ClusteredConnectionFactory");
         Queue queue0 = (Queue)ic0.lookup("/queue/testDistributedQueue");
         Queue queue1 = (Queue)ic1.lookup("/queue/testDistributedQueue");

         conn = cf.createConnection();

         // make sure we're connecting to node 1

         assertEquals(0, ((JBossConnection)conn).getServerID());

         // send a message

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue0);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         p.send(s.createTextMessage("blip"));

         conn.close();

         // create a connection to a different node

         conn = cf.createConnection();

         // make sure we're connecting to node 1

         assertEquals(1, ((JBossConnection)conn).getServerID());

         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c = s.createConsumer(queue1);
         conn.start();

         // we must receive the message

         TextMessage tm = (TextMessage)c.receive(10000);
         assertNotNull(tm);
         assertEquals("blip", tm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (ic0 != null)
         {
            ic0.close();
         }

         if (ic1 != null)
         {
            ic1.close();
         }

         ServerManagement.undeployQueue("testDistributedQueue", 0);
         ServerManagement.undeployQueue("testDistributedQueue", 1);

         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
