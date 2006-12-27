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
         conn.close();

         conn = cf.createConnection();
         conn.start();

         // create a producer/consumer on node 1

         // make sure we're connecting to node 1

         int nodeID = ((ConnectionState)((DelegateSupport)((JBossConnection)conn).
            getDelegate()).getState()).getServerID();

         assertEquals(1, nodeID);

         Session s1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c1 = s1.createConsumer(queue[1]);
         MessageProducer p1 = s1.createProducer(queue[1]);
         p1.setDeliveryMode(DeliveryMode.PERSISTENT);

         // send a message

         p1.send(s1.createTextMessage("blip"));

         // kill node 1


         log.info("########");
         log.info("######## KILLED NODE 1");
         log.info("########");

         ServerManagement.killAndWait(1);
         try
         {
            ic[1].lookup("queue"); // looking up anything
            fail("The server still alive, kill didn't work yet");
         }
         catch (Exception e)
         {
         }

         // TODO - this shouldn't be necessary if we have the client valve in place
         log.info("Sleeping for 60 sec");
         Thread.sleep(60000);

         // we must receive the message

         TextMessage tm = (TextMessage)c1.receive(1000);
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
