/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.JBossConnection;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedQueueTest extends ClusteringTestBase
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

   public void testNoop()
   {
   }

//   public void testForwardingMessageAmongNodes() throws Exception
//   {
//      Connection conn = null;
//
//      try
//      {
//         conn = cf.createConnection();
//
//         // send a message
//
//         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageProducer p = s.createProducer(queue[0]);
//         p.setDeliveryMode(DeliveryMode.PERSISTENT);
//         p.send(s.createTextMessage("blip"));
//
//         conn.close();
//
//         // create a connection to a different node
//
//         conn = cf.createConnection();
//
//         // make sure we're connecting to node 1
//
//         int nodeID = ((ConnectionState)((DelegateSupport)((JBossConnection)conn).
//            getDelegate()).getState()).getServerID();
//
//         assertEquals(1, nodeID);
//
//         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         MessageConsumer c = s.createConsumer(queue[1]);
//         conn.start();
//
//         // we must receive the message
//
//         TextMessage tm = (TextMessage)c.receive(1000);
//         assertEquals("blip", tm.getText());
//
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 2;

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
