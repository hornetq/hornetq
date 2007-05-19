/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceAttributeOverrides;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.plugin.RandomLoadBalancingPolicy;
import org.jboss.jms.client.plugin.RoundRobinLoadBalancingPolicy;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;
import javax.management.ObjectName;

/**
 * This test DOESN'T extend ClusteringTestBase because I want to have control over first invocations
 * to the server (which are outside of of my control if I use ClusteringTestBase).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class LoadBalancingTest extends MessagingTestCase
{

   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public LoadBalancingTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testRoundRobinLoadBalancingOneNode() throws Exception
   {
      // the round robin policy is default

      ServerManagement.start(0, "all", true);

      try
      {
         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(0));

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ClusteredConnectionFactory");

         JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
         ClientClusteredConnectionFactoryDelegate clusteredDelegate =
            (ClientClusteredConnectionFactoryDelegate )jbcf.getDelegate();

         assertSame(RoundRobinLoadBalancingPolicy.class,
            clusteredDelegate.getLoadBalancingPolicy().getClass());

         Connection conn0 = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn0).getServerID());

         Connection conn1 = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn1).getServerID());

         Connection conn2 = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn2).getServerID());

         conn0.close();
         conn1.close();
         conn2.close();

         ic.close();
      }
      finally
      {
         ServerManagement.stop(0);
      }
   }

   public void testRoundRobinLoadBalancingTwoNodes() throws Exception
   {
      // Make sure all servers are created and started; make sure that database is zapped ONLY for
      // the first server, the others rely on values they expect to find in shared tables; don't
      // clear the database for those.

      ServerManagement.start(0, "all", true);
      ServerManagement.start(1, "all", false);

      // the round robin policy is default

      try
      {
         InitialContext ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));

         ConnectionFactory cf = (ConnectionFactory)ic0.lookup("/ClusteredConnectionFactory");

         JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
         ClientClusteredConnectionFactoryDelegate clusteredDelegate =
            (ClientClusteredConnectionFactoryDelegate )jbcf.getDelegate();

         assertSame(RoundRobinLoadBalancingPolicy.class,
            clusteredDelegate.getLoadBalancingPolicy().getClass());

         Connection conn0 = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn0).getServerID());

         Connection conn1 = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn1).getServerID());

         Connection conn2 = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn2).getServerID());

         Connection conn3 = cf.createConnection();

         assertEquals(1, ((JBossConnection)conn3).getServerID());

         Connection conn4 = cf.createConnection();

         assertEquals(0, ((JBossConnection)conn4).getServerID());

         conn0.close();
         conn1.close();
         conn2.close();
         conn3.close();
         conn4.close();

         ic0.close();
      }
      finally
      {
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }

   public void testRandomRobinLoadBalancingSingleNode() throws Exception
   {
      // Make sure all servers are created and started; make sure that database is zapped ONLY for
      // the first server, the others rely on values they expect to find in shared tables; don't
      // clear the database for those.

      ServiceAttributeOverrides override = new ServiceAttributeOverrides();
      override.put(new ObjectName("jboss.messaging.connectionfactory:service=ClusteredConnectionFactory"),
         "LoadBalancingFactory", "org.jboss.jms.client.plugin.RandomLoadBalancingFactory");
      ServerManagement.start(0, "all", override, true);

      try
      {
         InitialContext ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));

         ConnectionFactory cf = (ConnectionFactory)ic0.lookup("/ClusteredConnectionFactory");

         JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
         ClientClusteredConnectionFactoryDelegate clusteredDelegate =
            (ClientClusteredConnectionFactoryDelegate )jbcf.getDelegate();

         assertSame(RandomLoadBalancingPolicy.class,
            clusteredDelegate.getLoadBalancingPolicy().getClass());

         Connection conn0 = cf.createConnection();
         assertEquals(0, getConnectionState(conn0).getServerID());

         Connection conn1 = cf.createConnection();
         assertEquals(0, getConnectionState(conn1).getServerID());

         Connection conn2 = cf.createConnection();
         assertEquals(0, getConnectionState(conn2).getServerID());

         Connection conn3 = cf.createConnection();
         assertEquals(0, getConnectionState(conn3).getServerID());

         Connection conn4 = cf.createConnection();
         assertEquals(0, getConnectionState(conn4).getServerID());

         conn0.close();
         conn1.close();
         conn2.close();
         conn3.close();
         conn4.close();

         ic0.close();
      }
      finally
      {
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }



   public void testRandomRobinLoadBalancingTwoNodes() throws Exception
   {
      // Make sure all servers are created and started; make sure that database is zapped ONLY for
      // the first server, the others rely on values they expect to find in shared tables; don't
      // clear the database for those.

      ServiceAttributeOverrides override = new ServiceAttributeOverrides();
      override.put(new ObjectName("jboss.messaging.connectionfactory:service=ClusteredConnectionFactory"),
         "LoadBalancingFactory", "org.jboss.jms.client.plugin.RandomLoadBalancingFactory");
      ServerManagement.start(0, "all", override, true);
      ServerManagement.start(1, "all", override, false);

      try
      {
         InitialContext ic0 = new InitialContext(ServerManagement.getJNDIEnvironment(0));

         ConnectionFactory cf = (ConnectionFactory)ic0.lookup("/ClusteredConnectionFactory");

         JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
         ClientClusteredConnectionFactoryDelegate clusteredDelegate =
            (ClientClusteredConnectionFactoryDelegate )jbcf.getDelegate();

         assertSame(RandomLoadBalancingPolicy.class,
            clusteredDelegate.getLoadBalancingPolicy().getClass());

         Connection conn0 = cf.createConnection();

         Connection conn1 = cf.createConnection();

         Connection conn2 = cf.createConnection();

         Connection conn3 = cf.createConnection();

         Connection conn4 = cf.createConnection();

         conn0.close();
         conn1.close();
         conn2.close();
         conn3.close();
         conn4.close();

         ic0.close();
      }
      finally
      {
         ServerManagement.stop(1);
         ServerManagement.stop(0);
      }
   }



   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected ConnectionState getConnectionState(Connection conn)
   {
      return (ConnectionState) (((DelegateSupport) ((JBossConnection) conn).
         getDelegate()).getState());
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
