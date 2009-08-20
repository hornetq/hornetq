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

import javax.jms.Connection;
import javax.jms.Session;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.jms.server.management.NullInitialContext;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A CloseConnectionOnGCTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class CloseConnectionOnGCTest extends UnitTestCase
{
   private HornetQServer server;

   private JMSServerManagerImpl jmsServer;

   private HornetQConnectionFactory cf;

   private static final String Q_NAME = "ConnectionTestQueue";

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      server = HornetQ.newMessagingServer(conf, false);
      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.setContext(new NullInitialContext());
      jmsServer.start();     
      jmsServer.createQueue(Q_NAME, Q_NAME, null, true);
      cf = new HornetQConnectionFactory(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));      
      cf.setBlockOnPersistentSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      jmsServer.stop();
      cf = null;
      if (server != null && server.isStarted())
      {
         try
         {
            server.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
         server = null;

      }
      
      server = null;
      jmsServer = null;
      cf = null;
      
      super.tearDown();
   }
   
   
   public void testCloseOneConnectionOnGC() throws Exception
   {
      Connection conn = cf.createConnection();
           
      assertEquals(1, server.getRemotingService().getConnections().size());
      
      conn = null;

      System.gc();
      System.gc();
      System.gc();
      
      Thread.sleep(2000);
                  
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
   
   public void testCloseSeveralConnectionOnGC() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();     
      
      assertEquals(3, server.getRemotingService().getConnections().size());
      
      conn1 = null;
      conn2 = null;
      conn3 = null;

      System.gc();
      System.gc();
      System.gc();
      
      Thread.sleep(2000);
                     
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
   
   public void testCloseSeveralConnectionsWithSessionsOnGC() throws Exception
   {
      Connection conn1 = cf.createConnection();
      Connection conn2 = cf.createConnection();
      Connection conn3 = cf.createConnection();    
      
      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess3 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess4 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess5 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess6 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess7 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
            
      assertEquals(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS, server.getRemotingService().getConnections().size());
      
      sess1 = sess2 = sess3 = sess4 = sess5 = sess6 = sess7 = null;
      
      conn1 = null;
      conn2 = null;
      conn3 = null;
      
      System.gc();
      System.gc();
      System.gc();
      
      Thread.sleep(2000);
                     
      assertEquals(0, server.getRemotingService().getConnections().size());
   }
   
}
