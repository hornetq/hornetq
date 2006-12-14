/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.messaging.core.plugin.contract.ClusteredPostOffice;

import javax.management.NotificationListener;
import javax.management.Notification;
import javax.management.ObjectName;
import java.util.Set;

import EDU.oswego.cs.dl.util.concurrent.Slot;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class GroupManagementTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public GroupManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testOneNodeCluster() throws Exception
   {
      try
      {
         ServerManagement.start("all", 0);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(0)));
      }
      finally
      {
         ServerManagement.stop(0);
      }
   }

   public void testJoinNotification() throws Exception
   {
      ClusterEventNotificationListener listener = new ClusterEventNotificationListener();
      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");

      try
      {
         ServerManagement.start("all", 0);

         log.info("Server 0 started");

         ServerManagement.addNotificationListener(0, postOfficeObjectName, listener);

         log.info("NotificationListener added to server 0");

         ServerManagement.start("all", 1);

         log.info("Blocking to receive notification ...");

         if (!listener.viewChanged(120000))
         {
            fail("Did not receive view change!");
         }

         Set view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
      }
      finally
      {
         ServerManagement.removeNotificationListener(0, postOfficeObjectName, listener);
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   public void testTwoNodesCluster() throws Exception
   {
      try
      {
         ServerManagement.start("all", 0);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(0)));

         ServerManagement.start("all", 1);

         view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));

         log.info("testTwoNodesCluster sucessful");
      }
      finally
      {
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   public void testThreeNodesCluster() throws Exception
   {
      try
      {
         ServerManagement.start("all", 0);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(0)));

         ServerManagement.start("all", 1);

         view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));

         ServerManagement.start("all", 3);

         view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(3, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(3)));

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(3, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(3)));

         view = ServerManagement.getServer(3).getNodeIDView();

         assertEquals(3, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(3)));

         log.info("testThreeNodesCluster sucessful");
      }
      finally
      {
         ServerManagement.stop(3);
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }


   public void testCleanLeave() throws Exception
   {
      try
      {
         // Start with a 3 node cluster

         ServerManagement.start("all", 0);
         ServerManagement.start("all", 1);
         ServerManagement.start("all", 2);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(3, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(2)));

         // Make node 0 to "cleanly" leave the cluster

         ServerManagement.stop(0);

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(2)));

         // Make node 2 to "cleanly" leave the cluster

         ServerManagement.stop(2);

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(1)));

         // Reuse the "hollow" RMI server 0 to start another cluster node

         ServerManagement.start("all", 0);

         view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));


         // Reuse the "hollow" RMI server 2 to start another cluster node

         ServerManagement.start("all", 2);

         view = ServerManagement.getServer(2).getNodeIDView();

         assertEquals(3, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(2)));

      }
      finally
      {
         ServerManagement.stop(2);
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   public void testDirtyLeaveOneNode() throws Exception
   {
      ClusterEventNotificationListener clusterEvent = new ClusterEventNotificationListener();
      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");

      try
      {
         // Start with a 2 node cluster

         ServerManagement.start("all", 0);
         ServerManagement.start("all", 1);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));

         ServerManagement.addNotificationListener(0, postOfficeObjectName, clusterEvent);

         // Make node 1 to "dirty" leave the cluster, by killing the VM running it.

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED 1");
         log.info("########");

         // Wait for membership change notification

         if (!clusterEvent.viewChanged(120000))
         {
            fail("Did not receive view change after killing server 2!");
         }

         view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(0)));
      }
      finally
      {
         ServerManagement.removeNotificationListener(0, postOfficeObjectName, clusterEvent);

         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   public void testDirtyLeaveTwoNodes() throws Exception
   {
      ClusterEventNotificationListener clusterEvent = new ClusterEventNotificationListener();
      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");

      try
      {
         // Start with a 3 node cluster

         ServerManagement.start("all", 0);
         ServerManagement.start("all", 1);
         ServerManagement.start("all", 2);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(3, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));
         assertTrue(view.contains(new Integer(2)));

         ServerManagement.addNotificationListener(0, postOfficeObjectName, clusterEvent);

         // Make node 2 to "dirty" leave the cluster, by killing the VM running it.

         ServerManagement.kill(2);

         log.info("########");
         log.info("######## KILLED 2");
         log.info("########");

         // Wait for FAILOVER_COMPLETED notification

         if (!clusterEvent.failoverCompleted(120000))
         {
            fail("Did not receive a FAILOVER_COMPLETED event after killing server 2!");
         }

         log.info("received FAILOVER_COMPLETED");

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(1)));

         // Make node 1 to "dirty" leave the cluster, by killing the VM running it.

         ServerManagement.kill(1);

         log.info("########");
         log.info("######## KILLED 1");
         log.info("########");

         // Wait for FAILOVER_COMPLETED notification

         if (!clusterEvent.failoverCompleted(120000))
         {
            fail("Did not receive a FAILOVER_COMPLETED event after killing server 1!");
         }

         log.info("received FAILOVER_COMPLETED");

         view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(0)));

      }
      finally
      {
         ServerManagement.removeNotificationListener(0, postOfficeObjectName, clusterEvent);

         ServerManagement.stop(2);
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   public void testSpawnServer() throws Exception
   {

      ObjectName postOfficeObjectName = new ObjectName("jboss.messaging:service=PostOffice");
      ClusterEventNotificationListener clusterEvent = new ClusterEventNotificationListener();

      try
      {
         // Start with a 1 node cluster

         ServerManagement.start("all", 0);

         Set view = ServerManagement.getServer(0).getNodeIDView();

         assertEquals(1, view.size());
         assertTrue(view.contains(new Integer(0)));

         ServerManagement.addNotificationListener(0, postOfficeObjectName, clusterEvent);

         ServerManagement.spawn(10);
         ServerManagement.start("all", 10);

         if (!clusterEvent.viewChanged(120000))
         {
            fail("Did not receive a VIEW_CHANGED event after spawning new server!");
         }

         view = ServerManagement.getServer(1).getNodeIDView();

         assertEquals(2, view.size());
         assertTrue(view.contains(new Integer(0)));
         assertTrue(view.contains(new Integer(10)));

         //ServerManagement.kill(10);

      }
      finally
      {
         ServerManagement.removeNotificationListener(0, postOfficeObjectName, clusterEvent);
         ServerManagement.stop(0);
      }
   }




   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class ClusterEventNotificationListener implements NotificationListener
   {
      private Slot viewChange;
      private Slot failoverCompleted;

      ClusterEventNotificationListener()
      {
         viewChange = new Slot();
         failoverCompleted = new Slot();
      }

      public void handleNotification(Notification notification, Object object)
      {
         String type = notification.getType();

         log.info("received " + type + " notification");

         if (ClusteredPostOffice.VIEW_CHANGED_NOTIFICATION.equals(type))
         {
            try
            {
               viewChange.put(Boolean.TRUE);
            }
            catch(InterruptedException e)
            {
               log.error(e);
            }
         }
         else if (ClusteredPostOffice.FAILOVER_COMPLETED_NOTIFICATION.equals(type))
         {
            try
            {
               failoverCompleted.put(Boolean.TRUE);
            }
            catch(InterruptedException e)
            {
               log.error(e);
            }
         }
         else
         {
            log.info("Ignoring notification " + type);
         }
      }

      public boolean viewChanged(long timeout) throws InterruptedException
      {
         Boolean result = (Boolean)viewChange.poll(timeout);
         if (result == null)
         {
            return false;
         }
         return result.booleanValue();
      }

      public boolean failoverCompleted(long timeout) throws InterruptedException
      {
         Boolean result = (Boolean)failoverCompleted.poll(timeout);
         if (result == null)
         {
            return false;
         }
         return result.booleanValue();
      }

   }

}
