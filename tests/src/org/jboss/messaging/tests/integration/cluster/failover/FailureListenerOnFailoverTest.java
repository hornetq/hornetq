/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A FailureListenerOnFailoverTest
 * 
 * Make sure FailuerListener is called at the right places during the failover process
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class FailureListenerOnFailoverTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FailureListenerOnFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   class MyListener implements FailureListener
   {
      private int i;
      
      MyListener(int i)
      {
         this.i = i;
      }
      
      int failCount;

      public synchronized boolean connectionFailed(final MessagingException me)
      {
         failCount++;
         
         return true;
      }
      
      synchronized int getFailCount()
      {
         return failCount;
      }
   }

   
   /*
    * Listeners shouldn't be called if failed over successfully
    */
   public void testFailureListenersNotCalledOnFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      Set<ClientSession> sessions = new HashSet<ClientSession>();
      
      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionImpl)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
         
         sessions.add(session);
      }

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(0, listener.getFailCount());
      }
      
      //Do some stuff to make sure sessions failed over/reconnected ok
      int i = 0;
      for (ClientSession session: sessions)
      {
         session.createQueue("testaddress" + i, "testaddress" + i, false);
         session.deleteQueue("testaddress" + i);
         i++;         
         session.close();
      }

      sf.close();
   }
   
   /*
    * Listeners shouldn't be called if reconnected successfully
    */
   public void testFailureListenersNotCalledOnReconnection() throws Exception
   {
      final long retryInterval = 10;

      final double retryMultiplier = 1d;

      final int initialConnectAttempts = 10;

      final int reconnectAttempts = 1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     initialConnectAttempts,
                                                                     reconnectAttempts);

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;
      
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionImpl)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
         
         sessions.add(session);
      }

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(0, listener.getFailCount());
      }
      
      //Do some stuff to make sure sessions failed over/reconnected ok
      int i = 0;
      for (ClientSession session: sessions)
      {
         session.createQueue("testaddress" + i, "testaddress" + i, false);
         session.deleteQueue("testaddress" + i);
         i++;         
         session.close();
      }

      sf.close();
   }

   /*
    * Listeners should be called if no backup server
    */
   public void testFailureListenerCalledNoBackup() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

     
      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionImpl)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
      }

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }
      
      sf.close();
   }

   /*
    * Listener should be called if failed to reconnect, no backup present
    */
   public void testFailureListenerCalledOnFailureToReconnect() throws Exception
   {
      final long retryInterval = 10;

      final double retryMultiplier = 1d;

      final int initialConnectAttempts = 1;

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     initialConnectAttempts,
                                                                     reconnectAttempts);

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionImpl)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
      }

      InVMConnector.failOnCreateConnection = true;
      
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      int i = 0;
      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }

      sf.close();
   }
   
   /*
    * Listener should be called if failed to reconnect, backup present
    */
   public void testFailureListenerCalledOnFailureToReconnectBackupPresent() throws Exception
   {
      final long retryInterval = 10;

      final double retryMultiplier = 1d;

      final int initialConnectAttempts = 1;

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     initialConnectAttempts,
                                                                     reconnectAttempts);

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;
      
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionImpl)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
         
         sessions.add(session);
      }
      
      //Fail once to failover ok

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      for (MyListener listener : listeners)
      {
         assertEquals(0, listener.getFailCount());
      }
      
      //Do some stuff to make sure sessions failed over/reconnected ok
      int i = 0;
      for (ClientSession session: sessions)
      {
         session.createQueue("testaddress" + i, "testaddress" + i, false);
         session.deleteQueue("testaddress" + i);
         i++;         
      }
      
      //Now fail again and reconnect ok
       
      ClientSession csession = sf.createSession(false, true, true);

      conn = ((ClientSessionImpl)csession).getConnection();
      
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;
                  
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      i = 0;
      for (ClientSession session: sessions)
      {
         session.createQueue("testaddress" + i, "testaddress" + i, false);
         session.deleteQueue("testaddress" + i);
         i++;         
      }
      
      //Now fail again and fail to reconnect
      
      csession = sf.createSession(false, true, true);

      conn = ((ClientSessionImpl)csession).getConnection();
      
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = -1;
                  
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      i = 0;
      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }
      
      csession.close();
            
      sf.close();
   }
   
   /*
    * Listener should be called if failed to failover
    */
   public void testFailureListenerCalledOnFailureToFailover() throws Exception
   {
      final long retryInterval = 10;

      final double retryMultiplier = 1d;

      final int initialConnectAttempts = 1;

      final int reconnectAttempts = 1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     initialConnectAttempts,
                                                                     reconnectAttempts);

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionImpl)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
      }
      
      InVMConnector.failOnCreateConnection = true;

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }

      sf.close();      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = Messaging.newNullStorageMessagingService(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = Messaging.newNullStorageMessagingService(liveConf);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      InVMConnector.resetFailures();

      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
