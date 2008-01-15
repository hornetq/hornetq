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

import EDU.oswego.cs.dl.util.concurrent.Latch;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientClusteredConnectionFactoryDelegate;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.ServiceAttributeOverrides;
import org.jboss.test.messaging.tools.container.ServiceContainer;

import javax.jms.*;
import javax.naming.InitialContext;

/**
 * Test situations where supports failover is marked false
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class NoFailoverTest extends ClusteringTestBase
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public NoFailoverTest(String name)
   {
      super(name);
   }
//
//   // Public ---------------------------------------------------------------------------------------
//
//   public void testCrashNoFailover() throws Exception
//   {
//      Connection conn = null;
//
//      try
//      {
//         assertFalse(((ClientClusteredConnectionFactoryDelegate)((JBossConnectionFactory)cf).getDelegate()).isSupportsFailover());
//         
//         log.info("dump 1");
//         ResourceManagerFactory.instance.dump();
//
//         conn = createConnectionOnServer(cf, 1);
//         
//         log.info("dump 2");
//         ResourceManagerFactory.instance.dump();
//
//      	MyListener listener = new MyListener();
//
//      	conn.setExceptionListener(listener);
//
//         assertEquals(1, getServerId(conn));
//
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         MessageProducer prod = sess.createProducer(queue[1]);
//
//         prod.send(sess.createTextMessage("Before Crash"));
//
//         //Now kill server 1
//
//         log.info("KILLING SERVER 1");
//         ServerManagement.kill(1);
//         log.info("KILLED SERVER 1");
//
//         JMSException e = listener.waitForException(20000);
//         
//         log.info("dump 3");
//         ResourceManagerFactory.instance.dump();
//
//         assertNotNull(e);
//
//         assertTrue(e.getMessage().equals("Failure on underlying remoting connection"));
//
//         // Connection should still be on server 1 (no client failover taken)
//         assertEquals(1, getServerId(conn));
//
//         //Now try and recreate connection on different node
//
//         conn.close();
//
//         conn = createConnectionOnServer(cf, 2); 
//
//         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         prod = sess.createProducer(queue[2]);
//
//         MessageConsumer cons = sess.createConsumer(queue[2]);
//
//         conn.start();
//
//         TextMessage tm = sess.createTextMessage("After Crash");
//
//         prod.send(tm);
//
//         TextMessage rm = (TextMessage)cons.receive(1000);
//
//         assertNotNull(rm);
//
//         assertEquals(tm.getText(), rm.getText());
//
//         rm = (TextMessage)cons.receive(1000);
//
//         assertNull(rm);
//
//         conn.close();
//
//         // Restarting the server
//         ServerManagement.start(1, "all", false);
//         deployQueue("testDistributedQueue", 1);
//         deployTopic("testDistributedTopic", 1);
//         InitialContext ic = getInitialContext();
//         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ClusteredConnectionFactory");
//
//         conn = createConnectionOnServer(cf, 1);
//
//         sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         cons = sess.createConsumer(queue[2]);
//
//         conn.start();
//
//         // message should still be on server.. no server failover taken
//         rm = (TextMessage) cons.receive(1000);
//
//         assertEquals(rm.getText(), "Before Crash");
//         
//         log.info("Got to end");
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
//   }
//
//
//   // Package protected ----------------------------------------------------------------------------
//
//   // Protected ------------------------------------------------------------------------------------
//
//   protected void setUp() throws Exception
//   {
//      this.nodeCount = 3;
//      
//      this.overrides = new ServiceAttributeOverrides();
//      
//      overrides.put(ServiceContainer.SERVER_PEER_OBJECT_NAME, "SupportsFailover", "false");
//            
//      super.setUp();
//   }
//
//   // Private --------------------------------------------------------------------------------------
//
//   // Inner classes --------------------------------------------------------------------------------
//   // Inner classes --------------------------------------------------------------------------------
//
//	private class MyListener implements ExceptionListener
//   {
//		private JMSException e;
//
//		Latch l = new Latch();
//
//		public void onException(JMSException e)
//		{
//			this.e = e;
//
//			l.release();
//		}
//
//		JMSException waitForException(long timeout) throws Exception
//		{
//			l.attempt(timeout);
//
//			return e;
//		}
//
//	}

}
