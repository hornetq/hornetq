/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.jms.connection;

import java.lang.ref.WeakReference;

import javax.jms.Connection;
import javax.jms.Session;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.tests.util.JMSTestBase;

/**
 * 
 * A CloseConnectionOnGCTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class CloseConnectionOnGCTest extends JMSTestBase
{
   private static final Logger log = Logger.getLogger(CloseConnectionOnGCTest.class);

   private HornetQConnectionFactory cf;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      cf = new HornetQConnectionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));      
      cf.setBlockOnPersistentSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      cf = null;
      
      super.tearDown();
   }
   
   public void testFoo() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         log.info("Iteration " + i);
         
         testCloseOneConnectionOnGC();
      }
   }
   
   public void testCloseOneConnectionOnGC() throws Exception
   {
      //Debug - don't remove this until intermittent failure with this test is fixed
      int initialConns = server.getRemotingService().getConnections().size();
      
      assertEquals(0, initialConns);
      
      
      Connection conn = cf.createConnection();
      
      WeakReference<Connection> wr = new WeakReference<Connection>(conn);
           
      assertEquals(1, server.getRemotingService().getConnections().size());
      
      conn = null;

      checkWeakReferences(wr);
                  
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
   
   public void testCloseSeveralConnectionOnGC() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();     
      
      WeakReference<Connection> wr1 = new WeakReference<Connection>(conn1);
      WeakReference<Connection> wr2 = new WeakReference<Connection>(conn2);
      WeakReference<Connection> wr3 = new WeakReference<Connection>(conn3);
      
      assertEquals(1, server.getRemotingService().getConnections().size());
      
      conn1 = null;
      conn2 = null;
      conn3 = null;

      checkWeakReferences(wr1, wr2, wr3);
                     
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
   
   public void testCloseSeveralConnectionsWithSessionsOnGC() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();   
      
      WeakReference<Connection> wr1 = new WeakReference<Connection>(conn1);
      WeakReference<Connection> wr2 = new WeakReference<Connection>(conn2);
      WeakReference<Connection> wr3 = new WeakReference<Connection>(conn3);
      
      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess3 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess4 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess5 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess6 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess7 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
      sess1 = sess2 = sess3 = sess4 = sess5 = sess6 = sess7 = null;
      
      conn1 = null;
      conn2 = null;
      conn3 = null;
      
      checkWeakReferences(wr1, wr2, wr3);
                     
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
}
