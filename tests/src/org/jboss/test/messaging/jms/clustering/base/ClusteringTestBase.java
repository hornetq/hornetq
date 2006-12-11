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

package org.jboss.test.messaging.jms.clustering.base;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision:$</tt>
 * $Id:$
 */
public class ClusteringTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Context ic0;
   protected Context ic1;
   protected Context ic2;

   protected Queue queue0;
   protected Queue queue1;
   protected Queue queue2;

   protected Topic topic0;
   protected Topic topic1;
   protected Topic topic2;

   //No need to have 3 conncetion factories since a clustered connection factory
   //will create connections in a round robin fashion on different servers
   protected ConnectionFactory cf;

   // Constructors --------------------------------------------------

   public ClusteringTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      String banner =
         "####################################################### Start " +
         (isRemote() ? "REMOTE" : "IN-VM") + " test: " + getName();

      ServerManagement.log(ServerManagement.INFO,banner);

      try
      {
         startServer(0);
         startServer(1);
         startServer(2);

         log.info("Deployed destinations ok");

         ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));
         ic1 = new InitialContext(ServerManagement.getJNDIEnvironment(1));
         ic2 = new InitialContext(ServerManagement.getJNDIEnvironment(2));

         //We only need to lookup one connection factory since it will be a clustered cf
         //so we will actually create connections on different servers (round robin)
         cf = (ConnectionFactory)ic0.lookup("/ConnectionFactory");

         queue0 = (Queue)ic0.lookup("queue/testDistributedQueue");
         queue1 = (Queue)ic1.lookup("queue/testDistributedQueue");
         queue2 = (Queue)ic2.lookup("queue/testDistributedQueue");

         topic0 = (Topic)ic0.lookup("topic/testDistributedTopic");
         topic1 = (Topic)ic1.lookup("topic/testDistributedTopic");
         topic2 = (Topic)ic2.lookup("topic/testDistributedTopic");

         drainQueues();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw e;
      }
   }

   protected void tearDown() throws Exception
   {
      try
      {

         if (ServerManagement.getServer(0).isStarted())
         {
            ServerManagement.log(ServerManagement.INFO, "Undeploying Server 0", 0);
            ServerManagement.undeployQueue("testDistributedQueue", 0);
            ServerManagement.undeployTopic("testDistributedTopic", 0);
         }

         if (ServerManagement.getServer(1).isStarted())
         {
            ServerManagement.log(ServerManagement.INFO, "Undeploying Server 1", 1);
            ServerManagement.undeployQueue("testDistributedQueue", 1);
            ServerManagement.undeployTopic("testDistributedTopic", 1);
         }

         if (ServerManagement.getServer(2).isStarted())
         {
            ServerManagement.log(ServerManagement.INFO, "Undeploying Server 2", 2);
            ServerManagement.undeployQueue("testDistributedQueue", 2);
            ServerManagement.undeployTopic("testDistributedTopic", 2);
         }

         ic0.close();
         ic1.close();
         ic2.close();

         super.tearDown();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         throw e;
      }
   }

   protected void checkConnectionsDifferentServers(Connection conn, Connection conn1, Connection conn2)
   {
      ConnectionState state0 =
         (ConnectionState)(((DelegateSupport)((JBossConnection)conn).getDelegate()).getState());
      ConnectionState state1 =
         (ConnectionState)(((DelegateSupport)((JBossConnection)conn1).getDelegate()).getState());
      ConnectionState state2 =
         (ConnectionState)(((DelegateSupport)((JBossConnection)conn2).getDelegate()).getState());

      int serverID0 = state0.getServerID();
      int serverID1 = state1.getServerID();
      int serverID2 = state2.getServerID();

      log.info("Server 0 ID: " + serverID0);
      log.info("Server 1 ID: " + serverID1);
      log.info("Server 2 ID: " + serverID2);

      assertTrue(serverID0 != serverID1);
      assertTrue(serverID1 != serverID2);
   }

   protected void drainQueues() throws Exception
   {
      Connection conn0 = null;
      Connection conn1 = null;
      Connection conn2 = null;

      try
      {
         //Since the cf is clustered, this will create connections on 3 different nodes
         //(round robin)
         conn0 = cf.createConnection();
         conn1 = cf.createConnection();
         conn2 = cf.createConnection();

         checkConnectionsDifferentServers(conn0, conn1, conn2);

         Session sess0 = conn0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons0 = sess0.createConsumer(queue0);
         MessageConsumer cons1 = sess1.createConsumer(queue1);
         MessageConsumer cons2 = sess2.createConsumer(queue2);

         conn0.start();
         conn1.start();
         conn2.start();

         Message msg = null;

         do
         {
            msg = cons0.receive(1000);
            log.info("1 Drained message " + msg);
         }
         while (msg != null);

         do
         {
            msg = cons1.receive(1000);
            log.info("2 Drained message " + msg);
         }
         while (msg != null);

         do
         {
            msg = cons2.receive(1000);
            log.info("3 Drained message " + msg);
         }
         while (msg != null);
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
      }
   }

   // Private -------------------------------------------------------

   private void startServer(int serverIndex) throws Exception
   {
      ServerManagement.start("all", serverIndex);
      ServerManagement.deployClusteredQueue("testDistributedQueue", serverIndex);
      ServerManagement.deployClusteredTopic("testDistributedTopic", serverIndex);
   }

   // Inner classes -------------------------------------------------

}
