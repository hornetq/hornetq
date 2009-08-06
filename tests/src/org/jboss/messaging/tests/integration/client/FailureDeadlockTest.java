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
package org.jboss.messaging.tests.integration.client;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.integration.jms.server.management.NullInitialContext;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A FailureDeadlockTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class FailureDeadlockTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FailureDeadlockTest.class);

   private MessagingServer server;

   private JMSServerManagerImpl jmsServer;

   private JBossConnectionFactory cf1;

   private JBossConnectionFactory cf2;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      server = Messaging.newMessagingServer(conf, false);
      jmsServer = new JMSServerManagerImpl(server);
      jmsServer.setContext(new NullInitialContext());
      jmsServer.start();
      cf1 = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      cf2 = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
   }

   @Override
   protected void tearDown() throws Exception
   {
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

   // https://jira.jboss.org/jira/browse/JBMESSAGING-1702
   //Test that two failures concurrently executing and calling the same exception listener
   //don't deadlock
   public void testDeadlock() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         final Connection conn1 = cf1.createConnection();

         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         RemotingConnection rc1 = ((ClientSessionImpl)((JBossSession)sess1).getCoreSession()).getConnection();

         final Connection conn2 = cf2.createConnection();

         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         RemotingConnection rc2 = ((ClientSessionImpl)((JBossSession)sess2).getCoreSession()).getConnection();

         ExceptionListener listener1 = new ExceptionListener()
         {
            public void onException(JMSException exception)
            {
               try
               {
                  conn2.close();
               }
               catch (Exception e)
               {
                  log.error("Failed to close connection2", e);
               }
            }
         };

         conn1.setExceptionListener(listener1);

         conn2.setExceptionListener(listener1);

         Failer f1 = new Failer(rc1);

         Failer f2 = new Failer(rc2);

         f1.start();

         f2.start();

         f1.join();

         f2.join();  
         
         conn1.close();
         
         conn2.close();
      }      
   }
   
   private class Failer extends Thread
   {
      RemotingConnection conn;

      Failer(RemotingConnection conn)
      {
         this.conn = conn;
      }

      public void run()
      {
         conn.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));
      }
   }

      
   //https://jira.jboss.org/jira/browse/JBMESSAGING-1703
   //Make sure that failing a connection removes it from the connection manager and can't be returned in a subsequent call
   public void testUsingDeadConnection() throws Exception
   {
      for (int i = 0; i < 100; i++)
      {
         final Connection conn1 = cf1.createConnection();
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         RemotingConnection rc1 = ((ClientSessionImpl)((JBossSession)sess1).getCoreSession()).getConnection();      
   
         rc1.fail(new MessagingException(MessagingException.NOT_CONNECTED, "blah"));
   
         Session sess2 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         conn1.close();
      }
   }

}
