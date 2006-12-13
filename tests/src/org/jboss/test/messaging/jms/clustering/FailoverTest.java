/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.delegate.DelegateSupport;

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
public class FailoverTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public FailoverTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSimpleFailover() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         // send a message

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue[0]);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         p.send(s.createTextMessage("blip"));

         conn.close();

         // create a connection to a node we'll kill soon

         conn = cf.createConnection();
         s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c = s.createConsumer(queue[2]); // TODO What happens if I use queue[1]?
         conn.start();

         // make sure we're connecting to node 1

         int nodeID =
            ((ConnectionState)((DelegateSupport)((JBossConnection)conn).getDelegate()).getState()).
               getServerID();

         assertEquals(1, nodeID);

         // kill node 1

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         // TODO - this shouldn't be necessary if we have the client valve in place
         log.info("Sleeping for 1 min");
         Thread.sleep(30000);

         // we must receive the message

         TextMessage tm = (TextMessage)c.receive(1000);
         assertEquals("blip", tm.getText());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 3;

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
