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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.UnitTestCase;

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

   private HornetQServer liveService;

   private HornetQServer backupService;

   private Map<String, Object> backupParams = new HashMap<String, Object>();

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

      public synchronized void connectionFailed(final HornetQException me)
      {
         failCount++;
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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
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
            conn = ((ClientSessionInternal)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);

         sessions.add(session);
      }

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(0, listener.getFailCount());
      }

      // Do some stuff to make sure sessions failed over/reconnected ok
      int i = 0;
      for (ClientSession session : sessions)
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

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      
      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionInternal)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);

         sessions.add(session);
      }

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(0, listener.getFailCount());
      }

      try
      {
         // Do some stuff to make sure sessions failed over/reconnected ok
         int i = 0;
         for (ClientSession session : sessions)
         {
            session.createQueue("testaddress" + i, "testaddress" + i, false);
            session.deleteQueue("testaddress" + i);
            i++;
            session.close();
         }
      }
      finally
      {
         sf.close();
      }
   }

   /*
    * Listeners should be called if no backup server
    */
   public void testFailureListenerCalledNoBackup() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;
      
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);
         
         sessions.add(session);

         if (conn == null)
         {
            conn = ((ClientSessionInternal)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
      }

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }
      
      for (ClientSession session : sessions)
      {
         session.close();
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

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;
      
      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);
         
         sessions.add(session);

         if (conn == null)
         {
            conn = ((ClientSessionInternal)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
      }

      InVMConnector.failOnCreateConnection = true;

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      int i = 0;
      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }
      
      for (ClientSession session : sessions)
      {
         session.close();
      }

      sf.close();
   }

   /*
    * Listener should be called if failed to reconnect after failover, backup present
    */
   public void testFailureListenerCalledOnFailureToReconnectBackupPresent() throws Exception
   {
      final long retryInterval = 10;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));
      
      sf.setFailoverOnServerShutdown(true);
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      
      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      Set<ClientSession> sessions = new HashSet<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);

         if (conn == null)
         {
            conn = ((ClientSessionInternal)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);

         sessions.add(session);
      }

      // Fail once to failover ok

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(0, listener.getFailCount());
      }

      // Do some stuff to make sure sessions failed over/reconnected ok
      int i = 0;
      for (ClientSession session : sessions)
      {
         session.createQueue("testaddress" + i, "testaddress" + i, false);
         session.deleteQueue("testaddress" + i);
         i++;
      }

      // Now fail again and reconnect ok

      ClientSession csession = sf.createSession(false, true, true);

      conn = ((ClientSessionInternal)csession).getConnection();

      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      i = 0;
      for (ClientSession session : sessions)
      {
         session.createQueue("testaddress" + i, "testaddress" + i, false);
         session.deleteQueue("testaddress" + i);
         i++;
      }
      
      csession.close();

      // Now fail again and fail to reconnect

      csession = sf.createSession(false, true, true);

      conn = ((ClientSessionInternal)csession).getConnection();

      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = -1;

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      i = 0;
      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }

      csession.close();
      
      for (ClientSession session : sessions)
      {
         session.close();
      }

      sf.close();
   }

   /*
    * Listener should be called if failed to failover
    */
   public void testFailureListenerCalledOnFailureToFailover() throws Exception
   {
      final long retryInterval = 10;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));
      
      sf.setFailoverOnServerShutdown(true);
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      final int numSessions = (int)(ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 1.5);

      List<MyListener> listeners = new ArrayList<MyListener>();

      RemotingConnection conn = null;

      Set<ClientSession> sessions = new HashSet<ClientSession>();
      
      for (int i = 0; i < numSessions; i++)
      {
         ClientSession session = sf.createSession(false, true, true);
         
         sessions.add(session);

         if (conn == null)
         {
            conn = ((ClientSessionInternal)session).getConnection();
         }

         MyListener listener = new MyListener(i);

         session.addFailureListener(listener);

         listeners.add(listener);
      }

      InVMConnector.failOnCreateConnection = true;

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (MyListener listener : listeners)
      {
         assertEquals(1, listener.getFailCount());
      }
      
      for (ClientSession session: sessions)
      {
         session.close();
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
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = HornetQ.newHornetQServer(backupConf, false);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = HornetQ.newHornetQServer(liveConf, false);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      InVMConnector.resetFailures();

      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      backupService = null;
      
      liveService = null;
      
      backupParams = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
