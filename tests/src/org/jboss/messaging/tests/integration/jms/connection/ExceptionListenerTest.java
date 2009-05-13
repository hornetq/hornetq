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

import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.integration.jms.server.management.NullInitialContext;
import org.jboss.messaging.tests.util.UnitTestCase;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * 
 * A ExceptionListenerTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ExceptionListenerTest extends UnitTestCase
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
      
      super.tearDown();
   }
   
   private class MyExceptionListener implements ExceptionListener
   {
      volatile int numCalls;
      
      public synchronized void onException(JMSException arg0)
      {
         numCalls++;
      }      
   }

   public void testListenerCalledForOneConnection() throws Exception
   {
      Connection conn = cf.createConnection();
      
      MyExceptionListener listener = new MyExceptionListener();
      
      conn.setExceptionListener(listener);
      
      ClientSessionInternal coreSession = (ClientSessionInternal)((JBossConnection)conn).getInitialSession();
      
      coreSession.getConnection().fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      
      assertEquals(1, listener.numCalls);                  
   }
   
   public void testListenerCalledForOneConnectionAndSessions() throws Exception
   {
      Connection conn = cf.createConnection();
      
      MyExceptionListener listener = new MyExceptionListener();
      
      conn.setExceptionListener(listener);
      
      Session sess1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Session sess2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Session sess3 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      ClientSessionInternal coreSession0 = (ClientSessionInternal)((JBossConnection)conn).getInitialSession();
      
      ClientSessionInternal coreSession1 = (ClientSessionInternal)((JBossSession)sess1).getCoreSession();
      
      ClientSessionInternal coreSession2 = (ClientSessionInternal)((JBossSession)sess2).getCoreSession();
      
      ClientSessionInternal coreSession3 = (ClientSessionInternal)((JBossSession)sess3).getCoreSession();
      
      coreSession0.getConnection().fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      
      coreSession1.getConnection().fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      
      coreSession2.getConnection().fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      
      coreSession3.getConnection().fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      
      //Listener should only be called once even if all sessions connections die
      assertEquals(1, listener.numCalls);                  
   }
   
}
