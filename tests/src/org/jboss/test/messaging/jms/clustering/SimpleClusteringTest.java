/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.jms.clustering.base.ClusteringTestBase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.TextMessage;

import EDU.oswego.cs.dl.util.concurrent.Slot;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleClusteringTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SimpleClusteringTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   /**
    * This test is an example how to programatically make a cluster node to join the cluser.
    */
   public void testJoin() throws Exception
   {
      // By this time, after running setUp(), we should have an already formed cluster of three
      // nodes (0, 1, 2).

      // TODO - verify this assertion

      // We start programatically the fourth node.

      ServerManagement.start("all", 3);

      // TODO - verify that the cluster formed correctly assertion

      ServerManagement.stop(3);
   }

   /**
    * This test is an example how to programatically make a cluster node to cleanly leave the
    * cluster.
    */
   public void testCleanLeave() throws Exception
   {
      // By this time, after running setUp(), we should have an already formed cluster of three
      // nodes (0, 1, 2).

      // TODO - verify this assertion

      // We get the first node to programatically leave the cluster

      ServerManagement.stop(0);

      // TODO - verify that the cluster formed correctly assertion

      // We get the last node to programatically leave the cluster

      ServerManagement.stop(2);

   }

   public void testLeaveAndJoin() throws Exception
   {
      // By this time, after running setUp(), we should have an already formed cluster of three
      // nodes (0, 1, 2).

      // TODO - verify this assertion

      // We get the first node to programatically leave the cluster

      ServerManagement.stop(0);

      // TODO - verify that the cluster formed correctly assertion

      // We get the first node to re-join

      ServerManagement.start("all", 0);

   }
   
   public void testDistributedTopic() throws Exception
   {
      Connection conn = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {
         conn = cf.createConnection();
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session s1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session s2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         SimpleMessageListener ml = new SimpleMessageListener();
         SimpleMessageListener ml1 = new SimpleMessageListener();
         SimpleMessageListener ml2 = new SimpleMessageListener();

         s.createConsumer(topic[0]).setMessageListener(ml);
         s1.createConsumer(topic[0]).setMessageListener(ml1);
         s2.createConsumer(topic[0]).setMessageListener(ml2);

         conn.start();
         conn1.start();
         conn2.start();

         s.createProducer(topic[0]).send(s.createTextMessage("boom"));

         TextMessage rm = null;

         rm = ml.poll(5000);
         assertEquals("boom", rm.getText());

         rm = ml1.poll(5000);
         assertEquals("boom", rm.getText());

         rm = ml2.poll(5000);
         assertEquals("boom", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
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

   private class SimpleMessageListener implements MessageListener
   {
      private Slot slot;

      SimpleMessageListener()
      {
         slot = new Slot();
      }

      public void onMessage(Message message)
      {
         try
         {
            slot.put(message);
         }
         catch(InterruptedException e)
         {
            log.error(e);
         }
      }

      public TextMessage poll(long timeout) throws InterruptedException
      {
         return (TextMessage)slot.poll(timeout);
      }
   }
}
