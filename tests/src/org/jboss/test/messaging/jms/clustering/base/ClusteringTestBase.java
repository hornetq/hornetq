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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision:$</tt>
 * $Id:$
 */
public class ClusteringTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected int nodeCount;

   protected Context[] ic;
   protected Queue queue[];
   protected Topic topic[];

   // No need to have multiple conncetion factories since a clustered connection factory will create
   // connections in a round robin fashion on different servers.

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

      if (nodeCount < 1)
      {
         throw new Exception("Node count not defined! Initalize nodeCount in the test's setUp()");
      }

      ic = new Context[nodeCount];
      queue = new Queue[nodeCount];
      topic = new Topic[nodeCount];

      for(int i = 0; i < nodeCount; i++)
      {
         ServerManagement.start("all", i);
         ServerManagement.deployClusteredQueue("testDistributedQueue", i);
         ServerManagement.deployClusteredTopic("testDistributedTopic", i);

         ic[i] = new InitialContext(ServerManagement.getJNDIEnvironment(i));
         queue[i] = (Queue)ic[i].lookup("queue/testDistributedQueue");
         topic[i] = (Topic)ic[i].lookup("topic/testDistributedTopic");
      }

      // We only need to lookup one connection factory since it will be clustered so we will
      // actually create connections on different servers (round robin).
      cf = (ConnectionFactory)ic[0].lookup("/ConnectionFactory");

      drainQueues();
   }

   protected void tearDown() throws Exception
   {

      for(int i = 0; i < nodeCount; i++)
      {
         if (ServerManagement.isStarted(i))
         {
            ServerManagement.log(ServerManagement.INFO, "Undeploying Server " + i, i);
            ServerManagement.undeployQueue("testDistributedQueue", i);
            ServerManagement.undeployTopic("testDistributedTopic", i);
         }

         ic[i].close();
      }

      super.tearDown();
   }

   protected void checkConnectionsDifferentServers(Connection[] conn) throws Exception
   {
      int[] serverID = new int[conn.length];
      for(int i = 0; i < conn.length; i++)
      {
         ConnectionState state = (ConnectionState)(((DelegateSupport)((JBossConnection)conn[i]).
            getDelegate()).getState());
         serverID[i] = state.getServerID();
      }

      for(int i = 0; i < nodeCount; i++)
      {
         for(int j = 0; j < nodeCount; j++)
         {
            if (i == j)
            {
               continue;
            }

            if (serverID[i] == serverID[j])
            {
               fail("Connections " + i + " and " + j +
                  " are pointing to the same physical node (" + serverID[i] + ")");
            }
         }
      }
   }

   // Private -------------------------------------------------------

   private void drainQueues() throws Exception
   {
      Connection[] conn = new Connection[nodeCount];

      try
      {
         // TODO This is a dangerous hack, relying on an arbitrary distribution algorithm
         // (round-robin in this case). If we want a connection to a specific node, we should be
         // able to look up something like "/ConnectionFactory0"

         for(int i = 0; i < nodeCount; i++)
         {
            conn[i] = cf.createConnection();
         }

         // Safety check, making sure we get connections to distinct nodes

         checkConnectionsDifferentServers(conn);

         for(int i = 0; i < nodeCount; i++)
         {
            Session s = conn[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer c = s.createConsumer(queue[i]);
            conn[i].start();

            Message msg = null;
            do
            {
               msg = c.receive(1000);
               if (msg != null)
               {
                  log.info("Drained message " + msg + " on node " + i);
               }
            }
            while (msg != null);
         }
      }
      finally
      {
         for(int i = 0; i < nodeCount; i++)
         {
            if (conn[i] != null)
            {
               conn[i].close();
            }
         }
      }
   }

   // Inner classes -------------------------------------------------

}
