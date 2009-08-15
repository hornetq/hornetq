/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.integration.jms.connection;

import javax.jms.Connection;
import javax.jms.Session;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.integration.jms.server.management.NullInitialContext;
import org.jboss.messaging.tests.util.UnitTestCase;

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
   private MessagingServer server;

   private JMSServerManagerImpl jmsServer;

   private JBossConnectionFactory cf;

   private static final String Q_NAME = "ConnectionTestQueue";

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      server = Messaging.newMessagingServer(conf, false);
      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.setContext(new NullInitialContext());
      jmsServer.start();     
      jmsServer.createQueue(Q_NAME, Q_NAME, null, true);
      cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));      
      cf.setBlockOnPersistentSend(true);
      cf.setPreAcknowledge(true);
   }

   @Override
   protected void tearDown() throws Exception
   {
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
