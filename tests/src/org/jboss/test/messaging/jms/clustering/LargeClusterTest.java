/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import java.util.Set;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Slot;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class LargeClusterTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public LargeClusterTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   /**
    * This test is an example how to programatically make a cluster node to join the cluser.
    */
   public void testJoin() throws Exception
   {
      // By this time, after running setUp(), we should have an already formed cluster of seven
      // nodes

      Set view = ServerManagement.getServer(0).getNodeIDView();

      assertEquals(7, view.size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(3)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));

      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");
      ClusterEventNotificationListener clusterEvent = new ClusterEventNotificationListener();

      ServerManagement.addNotificationListener(4, postOfficeObjectName, clusterEvent);

      // We start programatically the eighth node.

      ServerManagement.start(7, "all");

      // wait for change to propagate

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      view = ServerManagement.getServer(0).getNodeIDView();

      assertEquals(8, view.size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(3)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));
      assertTrue(view.contains(new Integer(7)));

      ServerManagement.removeNotificationListener(4, postOfficeObjectName, clusterEvent);

      ServerManagement.kill(7);
   }

   /**
    * This test is an example how to programatically make a cluster node to cleanly leave the
    * cluster.
    */
   public void testCleanLeave() throws Exception
   {
      // By this time, after running setUp(), we should have an already formed cluster of seven
      // nodes

      Set view = ServerManagement.getServer(0).getNodeIDView();

      assertEquals(7, view.size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(3)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));

      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");
      ClusterEventNotificationListener clusterEvent = new ClusterEventNotificationListener();

      ServerManagement.addNotificationListener(0, postOfficeObjectName, clusterEvent);

      // We get all nodes to programatically leave the cluster

      ServerManagement.stop(3);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      assertEquals(6, ServerManagement.getServer(0).getNodeIDView().size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));

      ServerManagement.stop(6);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      assertEquals(5, ServerManagement.getServer(0).getNodeIDView().size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));

      ServerManagement.stop(1);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      assertEquals(4, ServerManagement.getServer(0).getNodeIDView().size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));

      ServerManagement.stop(2);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      assertEquals(3, ServerManagement.getServer(0).getNodeIDView().size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));

      ServerManagement.stop(4);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      assertEquals(2, ServerManagement.getServer(0).getNodeIDView().size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(5)));

      ServerManagement.removeNotificationListener(0, postOfficeObjectName, clusterEvent);
      ServerManagement.addNotificationListener(5, postOfficeObjectName, clusterEvent);

      ServerManagement.stop(0);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      assertEquals(1, ServerManagement.getServer(5).getNodeIDView().size());
      assertTrue(view.contains(new Integer(5)));

      ServerManagement.removeNotificationListener(5, postOfficeObjectName, clusterEvent);

      ServerManagement.stop(5);
   }

   public void testLeaveAndJoin() throws Exception
   {
      // By this time, after running setUp(), we should have an already formed cluster of seven
      // nodes

      Set view = ServerManagement.getServer(0).getNodeIDView();

      assertEquals(7, view.size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(3)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));

      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");
      ClusterEventNotificationListener clusterEvent = new ClusterEventNotificationListener();

      ServerManagement.addNotificationListener(5, postOfficeObjectName, clusterEvent);

      // We get the first node to programatically leave the cluster

      ServerManagement.stop(0);

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      view = ServerManagement.getServer(4).getNodeIDView();

      assertEquals(6, view.size());
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(3)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));


      // We get the first node to re-join

      ServerManagement.start(0, "all");

      if (!clusterEvent.viewChanged(30000))
      {
         fail("Did not receive a VIEW_CHANGED event after spawning new server!");
      }

      view = ServerManagement.getServer(6).getNodeIDView();
      assertEquals(7, view.size());
      assertTrue(view.contains(new Integer(0)));
      assertTrue(view.contains(new Integer(1)));
      assertTrue(view.contains(new Integer(2)));
      assertTrue(view.contains(new Integer(3)));
      assertTrue(view.contains(new Integer(4)));
      assertTrue(view.contains(new Integer(5)));
      assertTrue(view.contains(new Integer(6)));

      ServerManagement.removeNotificationListener(5, postOfficeObjectName, clusterEvent);

   }

   public void testDistributedTopic() throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;
      Connection conn3 = null;
      Connection conn4 = null;
      Connection conn5 = null;
      Connection conn6 = null;


      try
      {
         conn0 = this.createConnectionOnServer(cf, 0);
         assertEquals(0, ((ClientConnectionDelegate)((JBossConnection)conn0).
            getDelegate()).getServerID());
         Session s0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m0 = new SimpleMessageListener();
         s0.createConsumer(topic[0]).setMessageListener(m0);
         conn0.start();

         conn1 = cf.createConnection();
         assertEquals(1, ((ClientConnectionDelegate)((JBossConnection)conn1).
            getDelegate()).getServerID());
         Session s1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m1 = new SimpleMessageListener();
         s1.createConsumer(topic[1]).setMessageListener(m1);
         conn1.start();

         conn2 = cf.createConnection();
         assertEquals(2, ((ClientConnectionDelegate)((JBossConnection)conn2).
            getDelegate()).getServerID());
         Session s2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m2 = new SimpleMessageListener();
         s2.createConsumer(topic[2]).setMessageListener(m2);
         conn2.start();

         conn3 = cf.createConnection();
         assertEquals(3, ((ClientConnectionDelegate)((JBossConnection)conn3).
            getDelegate()).getServerID());
         Session s3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m3 = new SimpleMessageListener();
         s3.createConsumer(topic[3]).setMessageListener(m3);
         conn3.start();

         conn4 = cf.createConnection();
         assertEquals(4, ((ClientConnectionDelegate)((JBossConnection)conn4).
            getDelegate()).getServerID());
         Session s4 = conn4.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m4 = new SimpleMessageListener();
         s4.createConsumer(topic[4]).setMessageListener(m4);
         conn4.start();

         conn5 = cf.createConnection();
         assertEquals(5, ((ClientConnectionDelegate)((JBossConnection)conn5).
            getDelegate()).getServerID());
         Session s5 = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m5 = new SimpleMessageListener();
         s5.createConsumer(topic[5]).setMessageListener(m5);
         conn5.start();

         conn6 = cf.createConnection();
         assertEquals(6, ((ClientConnectionDelegate)((JBossConnection)conn6).
            getDelegate()).getServerID());
         Session s6 = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
         SimpleMessageListener m6 = new SimpleMessageListener();
         s6.createConsumer(topic[6]).setMessageListener(m6);
         conn6.start();

         s3.createProducer(topic[3]).send(s3.createTextMessage("boom"));

         TextMessage rm = null;

         rm = m0.poll(5000);
         assertEquals("boom", rm.getText());

         rm = m1.poll(5000);
         assertEquals("boom", rm.getText());

         rm = m2.poll(5000);
         assertEquals("boom", rm.getText());

         rm = m3.poll(5000);
         assertEquals("boom", rm.getText());

         rm = m4.poll(5000);
         assertEquals("boom", rm.getText());

         rm = m5.poll(5000);
         assertEquals("boom", rm.getText());

         rm = m6.poll(5000);
         assertEquals("boom", rm.getText());

      }
      finally
      {
         if (conn0 != null)
         {
            conn0.close();
         }

         if (conn1 != null)
         {
            conn1.close();
         }

         if (conn2 != null)
         {
            conn2.close();
         }

         if (conn3 != null)
         {
            conn3.close();
         }

         if (conn4 != null)
         {
            conn4.close();
         }

         if (conn5 != null)
         {
            conn5.close();
         }

         if (conn6 != null)
         {
            conn6.close();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      nodeCount = 7;

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
