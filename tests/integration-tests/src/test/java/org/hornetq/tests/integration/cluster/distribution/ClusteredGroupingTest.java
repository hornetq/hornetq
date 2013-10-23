/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package org.hornetq.tests.integration.cluster.distribution;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.server.group.GroupingHandler;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.group.impl.GroupingHandlerConfiguration;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.impl.Response;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClusteredGroupingTest extends ClusterTestBase
{

   public void testGroupingGroupTimeout() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, -1, 2000, 500);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      final CountDownLatch latch = new CountDownLatch(4);

      getServer(1).getManagementService().addNotificationListener(new NotificationListener()
      {
         @Override
         public void onNotification(Notification notification)
         {
            if(notification.getType() == NotificationType.UNPROPOSAL)
            {
               latch.countDown();
            }
         }
      });
      getServer(2).getManagementService().addNotificationListener(new NotificationListener()
      {
         @Override
         public void onNotification(Notification notification)
         {
            if(notification.getType() == NotificationType.UNPROPOSAL)
            {
               latch.countDown();
            }
         }
      });
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));

      verifyReceiveAll(10, 0);

      removeConsumer(0);

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 0, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));

      verifyReceiveAll(10, 0);
   }

   public void testGroupingGroupTimeoutSendRemote() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0, -1, 2000, 500);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 0, false);

      final CountDownLatch latch = new CountDownLatch(4);

      getServer(1).getManagementService().addNotificationListener(new NotificationListener()
      {
         @Override
         public void onNotification(Notification notification)
         {
            if(notification.getType() == NotificationType.UNPROPOSAL)
            {
               latch.countDown();
            }
         }
      });
      getServer(2).getManagementService().addNotificationListener(new NotificationListener()
      {
         @Override
         public void onNotification(Notification notification)
         {
            if(notification.getType() == NotificationType.UNPROPOSAL)
            {
               latch.countDown();
            }
         }
      });
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(1, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));

      verifyReceiveAll(10, 0);

      removeConsumer(0);

      assertTrue(latch.await(5, TimeUnit.SECONDS));

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      sendWithProperty(2, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);

      sendWithProperty(1, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id2"));

      verifyReceiveAll(10, 0);
   }

   public void testGroupingSimple() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);


   }

   public void testGroupingBindingNotPresentAtStart() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id3"));

      verifyReceiveAll(1, 0, 1, 2);

      closeAllConsumers();
      closeSessionFactory(0);
      closeSessionFactory(1);
      closeSessionFactory(2);

      stopServers(0);

      startServers(0);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id3"));

      verifyReceiveAll(1, 0, 1, 2);
   }


   public void testGroupingBindingsRemoved() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id3"));

      closeAllConsumers();
      closeSessionFactory(0);
      closeSessionFactory(1);
      closeSessionFactory(2);

      stopServers(0);

      stopServers(1);

      startServers(0);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(2, isNetty());

      addConsumer(0, 0, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 1, 0, false);
      waitForBindings(2, "queues.testaddress", 1, 1, false);

      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendWithProperty(0, "queues.testaddress", 1, false, Message.HDR_GROUP_ID, new SimpleString("id3"));

      //check for 2 messages on 0
      verifyReceiveAll(1, 0);
      verifyReceiveAll(1, 0);

      //get the pinned message from 2
      addConsumer(1, 2, "queue0", null);

      verifyReceiveAll(1, 1);
   }

   public void testGroupingTimeout() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2, 1);

      startServers(0, 1, 2);

      setUpGroupHandler(new GroupingHandler()
      {
         public SimpleString getName()
         {
            return null;
         }

         @Override
         public void remove(SimpleString id, SimpleString groupId, int distance)
         {
            //To change body of implemented methods use File | Settings | File Templates.
         }

         @Override
         public void start() throws Exception
         {
            //To change body of implemented methods use File | Settings | File Templates.
         }

         @Override
         public void stop() throws Exception
         {
            //To change body of implemented methods use File | Settings | File Templates.
         }

         @Override
         public boolean isStarted()
         {
            return false;
         }

         public Response propose(final Proposal proposal) throws Exception
         {
            return null;
         }

         public void proposed(final Response response) throws Exception
         {
            System.out.println("ClusteredGroupingTest.proposed");
         }

         public void send(final Response response, final int distance) throws Exception
         {
            System.out.println("ClusteredGroupingTest.send");
         }

         public Response receive(final Proposal proposal, final int distance) throws Exception
         {
            return null;
         }

         public void onNotification(final Notification notification)
         {
            System.out.println("ClusteredGroupingTest.onNotification");
         }

         public void addGroupBinding(final GroupBinding groupBinding)
         {
            System.out.println("ClusteredGroupingTest.addGroupBinding");
         }

         public Response getProposal(final SimpleString fullID)
         {
            return null;
         }

         @Override
         public void awaitBindings()
         {
            //To change body of implemented methods use File | Settings | File Templates.
         }
      }, 0);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      try
      {
         sendWithProperty(1, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
         Assert.fail("should timeout");
      }
      catch (Exception e)
      {
         e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
      }

   }

   public void testGroupingSendTo2queues() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(1, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 0);

   }

   public void testGroupingSendTo3queues() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(1, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 0);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 0);

   }

   public void testGroupingSendTo3queuesRemoteArbitrator() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(0, 10, 1);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 1);
      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(20, 30, 1);
   }

   public void testGroupingSendTo3queuesNoConsumerOnLocalQueue() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);
      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      // addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 0, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 0);
      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(20, 30, 0);

   }

   public void testGroupingRoundRobin() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      sendInRange(0, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id2"));
      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, new SimpleString("id3"));
      verifyReceiveAllWithGroupIDRoundRobin(0, 10, 0, 1, 2);

   }

   public void testGroupingSendTo3queuesQueueRemoved() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      sendInRange(0, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      sendInRange(1, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 0);
      sendInRange(2, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(20, 30, 0);
      removeConsumer(0);
      removeConsumer(1);
      removeConsumer(2);
      deleteQueue(0, "queue0");
      deleteQueue(1, "queue0");
      deleteQueue(2, "queue0");
      createQueue(0, "queues.testaddress", "queue1", null, false);
      addConsumer(3, 0, "queue1", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      waitForBindings(2, "queues.testaddress", 1, 1, false);

      sendInRange(0, "queues.testaddress", 30, 40, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      verifyReceiveAllInRange(30, 40, 3);
      sendInRange(1, "queues.testaddress", 40, 50, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      verifyReceiveAllInRange(40, 50, 3);
      sendInRange(2, "queues.testaddress", 50, 60, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      verifyReceiveAllInRange(50, 60, 3);
      System.out.println("*****************************************************************************");
   }

   public void testGroupingSendTo3queuesPinnedNodeGoesDown() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, true, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(true, 0, 10, 0);

      closeAllConsumers();

      final CountDownLatch latch = new CountDownLatch(4);
      NotificationListener listener = new NotificationListener()
      {
         public void onNotification(final Notification notification)
         {
            if (NotificationType.BINDING_REMOVED == notification.getType())
            {
               if (notification.getProperties()
                  .getSimpleStringProperty(ManagementHelper.HDR_ADDRESS)
                  .toString()
                  .equals("queues.testaddress"))
               {
                  latch.countDown();
               }
            }
            else if (NotificationType.BINDING_ADDED == notification.getType())
            {
               if (notification.getProperties()
                  .getSimpleStringProperty(ManagementHelper.HDR_ADDRESS)
                  .toString()
                  .equals("queues.testaddress"))
               {
                  latch.countDown();
               }
            }
         }
      };
      getServer(0).getManagementService().addNotificationListener(listener);
      getServer(2).getManagementService().addNotificationListener(listener);

      stopServers(1);

      startServers(1);
      Assert.assertTrue("timed out waiting for bindings to be removed and added back", latch.await(5,
                                                                                                   TimeUnit.SECONDS));
      getServer(0).getManagementService().removeNotificationListener(listener);
      getServer(2).getManagementService().removeNotificationListener(listener);
      addConsumer(1, 2, "queue0", null);

      waitForBindings(2, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 2, 1, false);
      waitForBindings(0, "queues.testaddress", 2, 1, false);
      sendInRange(2, "queues.testaddress", 10, 20, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
      verifyReceiveAllInRange(10, 20, 1);

      System.out.println("*****************************************************************************");

   }

   public void testGroupingSendTo3queuesPinnedNodeGoesDownSendBeforeStop() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, true, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(true, 0, 10, 0);

      closeAllConsumers();

      sendInRange(2, "queues.testaddress", 10, 20, true, Message.HDR_GROUP_ID, new SimpleString("id1"));
      final CountDownLatch latch = new CountDownLatch(4);
      NotificationListener listener = new NotificationListener()
      {
         public void onNotification(final Notification notification)
         {
            if (NotificationType.BINDING_REMOVED == notification.getType())
            {
               if (notification.getProperties()
                  .getSimpleStringProperty(ManagementHelper.HDR_ADDRESS)
                  .toString()
                  .equals("queues.testaddress"))
               {
                  latch.countDown();
               }
            }
            else if (NotificationType.BINDING_ADDED == notification.getType())
            {
               if (notification.getProperties()
                  .getSimpleStringProperty(ManagementHelper.HDR_ADDRESS)
                  .toString()
                  .equals("queues.testaddress"))
               {
                  latch.countDown();
               }
            }
         }
      };
      getServer(0).getManagementService().addNotificationListener(listener);
      getServer(2).getManagementService().addNotificationListener(listener);

      stopServers(1);

      closeSessionFactory(1);

      startServers(1);

      setupSessionFactory(1, isNetty());

      Assert.assertTrue("timed out waiting for bindings to be removed and added back", latch.await(5,
                                                                                                   TimeUnit.SECONDS));
      getServer(0).getManagementService().removeNotificationListener(listener);
      getServer(2).getManagementService().removeNotificationListener(listener);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      verifyReceiveAllInRange(10, 20, 1);

   }

   public void testGroupingSendTo3queuesPinnedNodeGoesDownSendAfterRestart() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);

      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, true);
      createQueue(1, "queues.testaddress", "queue0", null, true);
      createQueue(2, "queues.testaddress", "queue0", null, true);

      addConsumer(0, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 0, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 0, true);

      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(1, "queues.testaddress", 2, 0, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);

      sendInRange(1, "queues.testaddress", 0, 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(0, 10, 0);
      final CountDownLatch latch = new CountDownLatch(4);
      NotificationListener listener = new NotificationListener()
      {
         public void onNotification(final Notification notification)
         {
            if (NotificationType.BINDING_REMOVED == notification.getType())
            {
               if (notification.getProperties()
                  .getSimpleStringProperty(ManagementHelper.HDR_ADDRESS)
                  .toString()
                  .equals("queues.testaddress"))
               {
                  latch.countDown();
               }
            }
            else if (NotificationType.BINDING_ADDED == notification.getType())
            {
               if (notification.getProperties()
                  .getSimpleStringProperty(ManagementHelper.HDR_ADDRESS)
                  .toString()
                  .equals("queues.testaddress"))
               {
                  latch.countDown();
               }
            }
         }
      };
      getServer(0).getManagementService().addNotificationListener(listener);
      getServer(2).getManagementService().addNotificationListener(listener);
      stopServers(1);

      closeSessionFactory(1);
      startServers(1);
      Assert.assertTrue("timed out waiting for bindings to be removed and added back", latch.await(5,
                                                                                                   TimeUnit.SECONDS));
      setupSessionFactory(1, isNetty());
      getServer(0).getManagementService().removeNotificationListener(listener);
      getServer(2).getManagementService().removeNotificationListener(listener);
      addConsumer(1, 1, "queue0", null);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(0, "queues.testaddress", 2, 1, false);
      waitForBindings(2, "queues.testaddress", 2, 1, false);
      sendInRange(2, "queues.testaddress", 10, 20, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAllInRange(10, 20, 1);

      sendInRange(0, "queues.testaddress", 20, 30, false, Message.HDR_GROUP_ID, new SimpleString("id1"));
      verifyReceiveAllInRange(20, 30, 1);

   }

   public void testGroupingMultipleQueuesOnAddress() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      createQueue(0, "queues.testaddress", "queue1", null, false);
      createQueue(1, "queues.testaddress", "queue1", null, false);
      createQueue(2, "queues.testaddress", "queue1", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      addConsumer(3, 0, "queue0", null);
      addConsumer(4, 1, "queue0", null);
      addConsumer(5, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 2, 2, true);
      waitForBindings(1, "queues.testaddress", 2, 2, true);
      waitForBindings(2, "queues.testaddress", 2, 2, true);

      waitForBindings(0, "queues.testaddress", 4, 4, false);
      waitForBindings(1, "queues.testaddress", 4, 4, false);
      waitForBindings(2, "queues.testaddress", 4, 4, false);

      sendWithProperty(0, "queues.testaddress", 10, false, Message.HDR_GROUP_ID, new SimpleString("id1"));

      verifyReceiveAll(10, 0);

   }

   public void testGroupingMultipleSending() throws Exception
   {
      setupServer(0, isFileStorage(), isNetty());
      setupServer(1, isFileStorage(), isNetty());
      setupServer(2, isFileStorage(), isNetty());

      setupClusterConnection("cluster0", "queues", false, 1, isNetty(), 0, 1, 2);

      setupClusterConnection("cluster1", "queues", false, 1, isNetty(), 1, 0, 2);

      setupClusterConnection("cluster2", "queues", false, 1, isNetty(), 2, 0, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.LOCAL, 0);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 1);
      setUpGroupHandler(GroupingHandlerConfiguration.TYPE.REMOTE, 2);

      startServers(0, 1, 2);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());
      setupSessionFactory(2, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);
      createQueue(2, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);
      addConsumer(2, 2, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);
      waitForBindings(2, "queues.testaddress", 1, 1, true);

      waitForBindings(0, "queues.testaddress", 2, 2, false);
      waitForBindings(1, "queues.testaddress", 2, 2, false);
      waitForBindings(2, "queues.testaddress", 2, 2, false);

      CountDownLatch latch = new CountDownLatch(1);
      Thread[] threads = new Thread[9];
      int range = 0;
      for (int i = 0; i < 9; i++, range += 10)
      {
         threads[i] = new Thread(new ThreadSender(range, range + 10, 1, new SimpleString("id" + i), latch, i < 8));
      }
      for (Thread thread : threads)
      {
         thread.start();
      }

      verifyReceiveAllWithGroupIDRoundRobin(0, 30, 0, 1, 2);
   }

   @Override
   protected void tearDown() throws Exception
   {
      closeAllConsumers();
      closeAllSessionFactories();
      closeAllServerLocatorsFactories();
      super.tearDown();
   }

   public boolean isNetty()
   {
      return true;
   }

   class ThreadSender implements Runnable
   {
      private final int msgStart;

      private final int msgEnd;

      private final SimpleString id;

      private final CountDownLatch latch;

      private final boolean wait;

      private final int node;

      public ThreadSender(final int msgStart,
                          final int msgEnd,
                          final int node,
                          final SimpleString id,
                          final CountDownLatch latch,
                          final boolean wait)
      {
         this.msgStart = msgStart;
         this.msgEnd = msgEnd;
         this.node = node;
         this.id = id;
         this.latch = latch;
         this.wait = wait;
      }

      public void run()
      {
         if (wait)
         {
            try
            {
               latch.await(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
               e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
            }
         }
         else
         {
            latch.countDown();
         }
         try
         {
            sendInRange(node, "queues.testaddress", msgStart, msgEnd, false, Message.HDR_GROUP_ID, id);
         }
         catch (Exception e)
         {
            e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
         }
      }
   }
}
