/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.clustering;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.DelegateSupport;
import org.jboss.jms.client.plugin.RoundRobinLoadBalancingPolicy;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * This test DOESN'T extend ClusteringTestBase because I want to have control over first invocations
 * to the server (which are outside of of my control if I use ClusteringTestBase).
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class LoadBalancingTest extends NewClusteringTestBase
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

      JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
      ClientClusteredConnectionFactoryDelegate clusteredDelegate =
         (ClientClusteredConnectionFactoryDelegate )jbcf.getDelegate();

      assertSame(RoundRobinLoadBalancingPolicy.class,
         clusteredDelegate.getLoadBalancingPolicy().getClass());

      Connection conn0 = cf.createConnection();
      
      final int oneId = getServerId(conn0);
      final int otherId = 1 - oneId;

      Connection conn1 = cf.createConnection();
      
      assertEquals(otherId, getServerId(conn1));

      Connection conn2 = cf.createConnection();
      
      assertEquals(oneId, getServerId(conn2));

      conn0.close();
      conn1.close();
      conn2.close();
   }
   

   public void testRoundRobinLoadBalancingTwoNodes() throws Exception
   {
      JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
      ClientClusteredConnectionFactoryDelegate clusteredDelegate =
         (ClientClusteredConnectionFactoryDelegate )jbcf.getDelegate();

      assertSame(RoundRobinLoadBalancingPolicy.class,
         clusteredDelegate.getLoadBalancingPolicy().getClass());

      Connection conn0 = cf.createConnection();
      
      final int oneId = getServerId(conn0);
      final int otherId = 1 - oneId;

      assertEquals(oneId, getServerId(conn0));

      Connection conn1 = cf.createConnection();

      assertEquals(otherId, getServerId(conn1));

      Connection conn2 = cf.createConnection();

      assertEquals(oneId, getServerId(conn2));

      Connection conn3 = cf.createConnection();

      assertEquals(otherId, getServerId(conn3));

      Connection conn4 = cf.createConnection();

      assertEquals(oneId, getServerId(conn4));

      conn0.close();
      conn1.close();
      conn2.close();
      conn3.close();
      conn4.close();
   }
   
   public void testRoundRobinLoadBalancingStartsWithRandomNode() throws Exception
   {
      // Make sure all servers are created and started; make sure that database is zapped ONLY for
      // the first server, the others rely on values they expect to find in shared tables; don't
      // clear the database for those.

      int[] counts = new int[2];
      
      for (int i = 0; i < 10; i++)
      {
         InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment(0));
         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ClusteredConnectionFactory");
         Connection firstConnection = cf.createConnection();
         int serverPeerID = getServerId(firstConnection);
         firstConnection.close();
         ic.close();

         counts[serverPeerID]++;
      }
      
      assertTrue("Should have connected to ServerPeer 0 at least once", counts[0] > 0);
      assertTrue("Should have connected to ServerPeer 1 at least once", counts[1] > 0);
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
   	nodeCount = 2;
      super.setUp();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
