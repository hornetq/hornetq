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

package org.jboss.test.messaging.jms.clustering;

import java.lang.ref.WeakReference;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.delegate.TopologyResult;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClusterViewUpdateTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ClusterViewUpdateTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
   	nodeCount = 2;
   	
   	super.setUp();
   }

   /** This method is to make sure CFs are being released on GC, validating if the callbacks
    *  are not making any hard references */
   public void testUpdateTopology() throws Throwable
   {

      JBossConnectionFactory cf = (JBossConnectionFactory)ic[0].lookup("/ClusteredConnectionFactory");
      ClientClusteredConnectionFactoryDelegate clusterDelegate = (ClientClusteredConnectionFactoryDelegate)cf.getDelegate();
      assertEquals(2, clusterDelegate.getDelegates().length);
      clusterDelegate.getTopology();
      assertEquals(2, clusterDelegate.getDelegates().length);

      // Kill the same node as the CF is connected to
      ServerManagement.kill(1);
      Thread.sleep(5000);
      assertEquals(1, clusterDelegate.getDelegates().length);
      TopologyResult topology = clusterDelegate.getTopology();
      assertEquals(1, topology.getDelegates().length);

      clusterDelegate.closeCallback();
   }


   /** This method is to make sure CFs are being released on GC, validating if the callbacks
    *  are not making any hard references.
    *
    * Note: Even though this test is looking for memory leaks, it's important this test
    *       runs as part of the validation of ConnectionFactory updates, as a leak on CF would
    *       cause problems with excessive number of connections.
    *  */
  public void testGarbageCollectionOnClusteredCF() throws Throwable
   {

      JBossConnectionFactory cf = (JBossConnectionFactory)ic[0].lookup("/ClusteredConnectionFactory");
      ClientClusteredConnectionFactoryDelegate clusterDelegate = (ClientClusteredConnectionFactoryDelegate)cf.getDelegate();
      WeakReference<ClientClusteredConnectionFactoryDelegate> ref = new WeakReference<ClientClusteredConnectionFactoryDelegate>(clusterDelegate);

      // Using a separate block, as everything on this block has to be released (no references from the method)
      {
         Connection conn = cf.createConnection();
         conn.start();
         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer prod = session.createProducer(queue[0]);
         MessageConsumer cons = session.createConsumer(queue[0]);
         prod.send(session.createTextMessage("Hello"));
         session.commit();
         TextMessage message = (TextMessage)cons.receive(1000);
         assertEquals("Hello", message.getText());
         log.info("Received message " + message.getText());
         session.commit();
         conn.close();
      }


      // cf = null;
      clusterDelegate = null;

      int loops = 0;
      // Stays on loop until GC is done
      while (ref.get()!=null)
      {
         for (int i=0;i<10000;i++)
         {
            /// Just throwing extra garbage on the memory.. to make sure GC will happen
            
            String str = "GARBAGE GARBAGE GARBAGE GARBAGE GARBAGE GARBAGE GARBAGE " + i;
         }

         log.info("Calling system.gc()");
         System.gc();
         Thread.sleep(1000);
         if (loops++ > 10)
         {
            // This should be more than enough already... even the object wasn't cleared on more than
            // 2 GC cycles we have a leak already.

            // Note! Due to AOP references the CFDelegate will be released from AOP instances
            //       only after 2 or more GC cycles.
            break;
         }
      }

        // Case there are still references to the ConnectionFactory, uncomment this code,
        //  add -agentlib:jbossAgent to your JVM arguments (with jboss-profiler lib on path or LD_LIBRARY_PATH)
        //  and this will tell you where the code is leaking.
//      org.jboss.profiler.jvmti.JVMTIInterface jvmti = new org.jboss.profiler.jvmti.JVMTIInterface();
//
//
//      if (ref.get() != null)
//      {
//         if (jvmti.isActive())
//         {
//            log.info("There are still references to CF");
//            HashMap refMap = jvmti.createIndexMatrix();
//            log.info(jvmti.exploreObjectReferences(refMap, ref.get(), 5, true));
//         }
//         else
//         {
//            log.info("Profiler is not active");
//         }
//      }
//
      assertNull("There is a memory leak on ClientClusteredConnectionFactoryDelegate", ref.get());
   }
   
   public void testUpdateConnectionFactoryWithNoConnections() throws Exception
   {

      ServerManagement.kill(1);
   	
   	Thread.sleep(5000);
   	
   	Connection conn = createConnectionOnServer(cf, 0);
      try
      {

         ClientClusteredConnectionFactoryDelegate cfDelegate =
            (ClientClusteredConnectionFactoryDelegate)cf.getDelegate();

         assertEquals(1, cfDelegate.getDelegates().length);
      }
      finally
      {
         conn.close();
      }
   }
   
   public void testUpdateConnectionFactoryWithNoInitialConnections() throws Exception
   {
      try
      {
         ClientClusteredConnectionFactoryDelegate clusterDelegate = (ClientClusteredConnectionFactoryDelegate)this.cf.getDelegate();

         //JBossConnectionFactory cf2 = (JBossConnectionFactory)ic[0].lookup("/ClusteredConnectionFactory");
         //ClientClusteredConnectionFactoryDelegate clusterDelegate2 = (ClientClusteredConnectionFactoryDelegate)cf2.getDelegate();

         //We kill all the servers - this tests the connection factory's ability to create a first connection
         //when its entire cached set of delegates is stale

         startDefaultServer(2, currentOverrides, false);

         assertEquals(3, clusterDelegate.getDelegates().length);

         // assertEquals(3, clusterDelegate2.getDelegates().length);
         log.info("#################################### Killing server 1 and 0");
         ServerManagement.log(ServerManagement.INFO, "Killing server1", 2);
         ServerManagement.kill(1);
         // Need some time for Lease 
         Thread.sleep(5000);

         assertEquals("Delegates are different on topology", 2,clusterDelegate.getTopology().getDelegates().length);
         assertEquals(2, clusterDelegate.getDelegates().length);

         ServerManagement.log(ServerManagement.INFO, "Stopping server0", 2);
         ServerManagement.stop(0);

         Thread.sleep(1000);

         assertEquals(1, clusterDelegate.getDelegates().length);

         Connection conn = createConnectionOnServer(cf, 2);

         ClientClusteredConnectionFactoryDelegate cfDelegate =
            (ClientClusteredConnectionFactoryDelegate)cf.getDelegate();

         assertEquals(1, cfDelegate.getDelegates().length);

         conn.close();
      }
      finally
      {
         ServerManagement.kill(2);
      }
   }


   /** Case the updateCF is not captured, the hopping should run nicely.
    *  This test will disable CF callback for a connection and validate if hoping is working*/
   public void testNoUpdateCaptured() throws Exception
   {
      JBossConnectionFactory cfNoCallback = (JBossConnectionFactory)ic[0].lookup("/ClusteredConnectionFactory");
      ClientClusteredConnectionFactoryDelegate noCallbackDelegate =  (ClientClusteredConnectionFactoryDelegate )cfNoCallback.getDelegate();
      noCallbackDelegate.closeCallback();

      ServerManagement.kill(1);

      Connection conn = null;

      for (int i=0; i<4; i++)
      {
         try
         {
            conn = cfNoCallback.createConnection();
            // 0 is the only server alive, so.. all connection should be performed on it
            assertEquals(0, getServerId(conn));
         }
         finally
         {
            try
            {
               if (conn != null)
               {
                  conn.close();
               }
            }
            catch (Throwable ignored)
            {
            }
         }
      }

   }
   
   public void testUpdateConnectionFactoryOnKill() throws Exception
   {
      Connection conn = createConnectionOnServer(cf, 0);

      JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;

      ClientClusteredConnectionFactoryDelegate cfDelegate =
         (ClientClusteredConnectionFactoryDelegate)jbcf.getDelegate();

      assertEquals(2, cfDelegate.getDelegates().length);

      Connection conn1 = cf.createConnection();

      assertEquals(1, getServerId(conn1));

      ServerManagement.kill(1);

      log.info("sleeping 5 secs ...");
      Thread.sleep(5000);

      // first part of the test, verifies if the CF was updated
      assertEquals(1, cfDelegate.getDelegates().length);
      conn.close();

      log.info("sleeping 5 secs ...");
      Thread.sleep(5000);

      // Second part, verifies a possible race condition on failoverMap and handleFilover

      log.info("ServerId=" + getServerId(conn1));
      assertEquals(0, getServerId(conn1));
       
      //restart
      log.info("Restarting server");
      ServerManagement.start(1, "all", false);
      
      Thread.sleep(5000);
      
      assertEquals(2, cfDelegate.getDelegates().length);
      
      conn1.close();
            
      ServerManagement.stop(1);
   }
   
   public void testUpdateConnectionFactoryOnStop() throws Exception
   {
      Connection conn = createConnectionOnServer(cf, 0);

      JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;

      ClientClusteredConnectionFactoryDelegate cfDelegate =
         (ClientClusteredConnectionFactoryDelegate)jbcf.getDelegate();

      assertEquals(2, cfDelegate.getDelegates().length);

      Connection conn1 = cf.createConnection();

      assertEquals(1, getServerId(conn1));

      ServerManagement.kill(1);

      log.info("sleeping 5 secs ...");
      Thread.sleep(5000);

      // first part of the test, verifies if the CF was updated
      assertEquals(1, cfDelegate.getDelegates().length);
      conn.close();

      log.info("sleeping 5 secs ...");
      Thread.sleep(5000);

      // Second part, verifies a possible race condition on failoverMap and handleFilover
      assertEquals(0, getServerId(conn1));
     
      //restart
      ServerManagement.start(1, "all", false);
      
      Thread.sleep(5000);
      
      assertEquals(2, cfDelegate.getDelegates().length);
      
      conn1.close();
      
      ServerManagement.stop(1);
   }

//   public void testUpdateMixedConnectionFactory() throws Exception
//   {
//      Connection conn = createConnectionOnServer(cf, 0);
//      JBossConnectionFactory jbcf = (JBossConnectionFactory)cf;
//
//      ClientClusteredConnectionFactoryDelegate cfDelegate =
//         (ClientClusteredConnectionFactoryDelegate)jbcf.getDelegate();
//
//      assertEquals(2, cfDelegate.getDelegates().length);
//
//      ConnectionFactory httpCF = (ConnectionFactory)ic[0].lookup("/HTTPConnectionFactory");
//      JBossConnectionFactory jbhttpCF = (JBossConnectionFactory) httpCF;
//      
//      Connection httpConn = createConnectionOnServer(httpCF, 0);
//
//      ClientClusteredConnectionFactoryDelegate httpcfDelegate =
//         (ClientClusteredConnectionFactoryDelegate)jbhttpCF.getDelegate();
//
//      assertEquals(2, httpcfDelegate.getDelegates().length);
//
//      validateCFs(cfDelegate, httpcfDelegate);
//
//      Connection conn1 = cf.createConnection();
//      Connection httpConn1 = httpCF.createConnection();
//
//      assertEquals(1, getServerId(conn1));
//      assertEquals(1, getServerId(httpConn1));
//
//      ServerManagement.kill(1);
//
//      log.info("sleeping 5 secs ...");
//      Thread.sleep(5000);
//
//      // first part of the test, verifies if the CF was updated
//      assertEquals(1, cfDelegate.getDelegates().length);
//      assertEquals(1, httpcfDelegate.getDelegates().length);
//
//      validateCFs(cfDelegate, httpcfDelegate);
//
//      conn.close();
//      httpConn.close();
//
//      log.info("sleeping 5 secs ...");
//      Thread.sleep(5000);
//
//      // Second part, verifies a possible racing condition on failoverMap and handleFilover
//
//      log.info("ServerId=" + getServerId(conn1));
//      assertEquals(0, getServerId(conn1));
//      assertEquals(0, getServerId(httpConn1));
//
//      conn1.close();
//      httpConn1.close();
//   }

   /**
    * Test if an update on failoverMap on the connectionFactory would
    * cause any problems during failover
    */
   public void testUpdateConnectionFactoryRaceCondition() throws Exception
   {
      // This connection needs to be opened, as we need the callback to update CF from this conn
      Connection conn = createConnectionOnServer(cf, 0);
      JBossConnectionFactory jbcf = (JBossConnectionFactory) cf;
      ClientClusteredConnectionFactoryDelegate cfDelegate =
         (ClientClusteredConnectionFactoryDelegate) jbcf.getDelegate();
      assertEquals(2, cfDelegate.getDelegates().length);

      Connection conn1 = cf.createConnection();

      Connection conn2 = cf.createConnection();

      assertEquals(1, getServerId(conn1));
      
      assertEquals(0, getServerId(conn2));

      ConnectionState state = this.getConnectionState(conn1);

      // Disable Leasing for Failover
      state.getRemotingConnection().removeConnectionListener();

      ServerManagement.kill(1);

      Thread.sleep(5000);

      // This will force Failover from Valve to kick in
      conn1.createSession(true, Session.SESSION_TRANSACTED);

      // first part of the test, verifies if the CF was updated
      assertEquals(1, cfDelegate.getDelegates().length);

      assertEquals(0, getServerId(conn1));
      
      conn.close();
      conn1.close();
      conn2.close();

   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Validate if two distinct CFs are valid
   private void validateCFs(ClientClusteredConnectionFactoryDelegate cfDelegate,
                            ClientClusteredConnectionFactoryDelegate httpcfDelegate)
   {
      ClientConnectionFactoryDelegate delegatesSocket[] = cfDelegate.getDelegates();
      ClientConnectionFactoryDelegate delegatesHTTP[] = httpcfDelegate.getDelegates();

      log.info("ValidateCFs:");

      assertEquals(delegatesSocket.length, delegatesHTTP.length);
      for (int i=0;i<delegatesSocket.length;i++)
      {
         log.info("DelegateSocket[" + i + "]=" + delegatesSocket[i].getServerLocatorURI());
         log.info("DelegateHttp[" + i + "]=" + delegatesHTTP[i].getServerLocatorURI());
         for (int j=0;j<delegatesHTTP.length;j++)
         {
            assertFalse(delegatesSocket[i].getServerLocatorURI().
               equals(delegatesHTTP[j].getServerLocatorURI()));
         }
      }
   }

   // Inner classes --------------------------------------------------------------------------------

}
