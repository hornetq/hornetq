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

import java.util.HashMap;
import java.util.Map;

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

   /*
    * Listener shouldn't be called if failed over successfully
    */
   public void testFailureListenerNotCalledOnFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      ClientSession session = sf.createSession(false, true, true);

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      class MyListener implements FailureListener
      {
         volatile boolean listenerCalled;
         
         public boolean connectionFailed(final MessagingException me)
         {
            log.info("** calling my failure listener");
            listenerCalled = true;
            
            return true;
         }
      }
      
      MyListener listener = new MyListener();

      session.addFailureListener(listener);
      
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
            
      assertFalse(listener.listenerCalled);

      session.close();

      sf.close();
   }
   
   /*
    * Listener should be called if no backup server or reconnect
    */
   public void testFailureListenerCalledNoFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true);

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      class MyListener implements FailureListener
      {
         volatile boolean listenerCalled;
         
         public boolean connectionFailed(final MessagingException me)
         {
            log.info("** calling my failure listener");
            listenerCalled = true;
            
            return true;
         }
      }
      
      MyListener listener = new MyListener();

      session.addFailureListener(listener);
      
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
            
      assertTrue(listener.listenerCalled);

      session.close();

      sf.close();
   }
   
   /*
    * Listener should be called if failed to connect before failover
    */
   public void testFailureListenerCalledOnFailureToReconnectBeforeFailover() throws Exception
   {
      final long retryInterval = 250;

      final double retryMultiplier = 1d;

      final int maxRetriesBeforeFailover = 1;

      final int maxRetriesAfterFailover = 0;
      
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                                                retryInterval,
                                                                                                retryMultiplier,
                                                                                                maxRetriesBeforeFailover,
                                                                                                maxRetriesAfterFailover);

      ClientSession session = sf.createSession(false, true, true);

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      class MyListener implements FailureListener
      {
         volatile boolean listenerCalled;
         
         public boolean connectionFailed(final MessagingException me)
         {
            listenerCalled = true;
            
            return true;
         }
      }
      
      MyListener listener = new MyListener();

      session.addFailureListener(listener);

      InVMConnector.failOnCreateConnection = true;
      
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      Thread.sleep(retryInterval * 2);
      
      assertTrue(listener.listenerCalled);

      session.close();

      sf.close();
   }
   
   /*
    * Listener should be called if failed to connect after failover
    */
   public void testFailureListenerCalledOnFailureToReconnectAfterFailover() throws Exception
   {
      log.info("Starting 2nd test");
      
      final long retryInterval = 250;

      final double retryMultiplier = 1d;

      final int maxRetriesBeforeFailover = 0;

      final int maxRetriesAfterFailover = 1;
      
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                                                retryInterval,
                                                                                                retryMultiplier,
                                                                                                maxRetriesBeforeFailover,
                                                                                                maxRetriesAfterFailover);

      ClientSession session = sf.createSession(false, true, true);

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      class MyListener implements FailureListener
      {
         volatile boolean listenerCalled;
         
         public boolean connectionFailed(final MessagingException me)
         {
            listenerCalled = true;
            
            return true;
         }
      }
      
      MyListener listener = new MyListener();

      session.addFailureListener(listener);

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      assertFalse(listener.listenerCalled);
      
      InVMConnector.failOnCreateConnection = true;
           
      log.info("Failing again");
      
      conn = ((ClientSessionImpl)session).getConnection();
      
      //Now fail again
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      Thread.sleep(retryInterval * 2);
      
      assertTrue(listener.listenerCalled);

      session.close();

      sf.close();
   }
   
   /*
    * Listener should be called if failed to connect before failover
    */
   public void testFailureListenerCalledOnStraightFailureToReconnect() throws Exception
   {
      final long retryInterval = 250;

      final double retryMultiplier = 1d;

      final int maxRetriesBeforeFailover = 1;

      final int maxRetriesAfterFailover = 0;
      
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),                                                                     
                                                                                                retryInterval,
                                                                                                retryMultiplier,
                                                                                                maxRetriesBeforeFailover,
                                                                                                maxRetriesAfterFailover);

      ClientSession session = sf.createSession(false, true, true);

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      class MyListener implements FailureListener
      {
         volatile boolean listenerCalled;
         
         public boolean connectionFailed(final MessagingException me)
         {
            listenerCalled = true;
            
            return true;
         }
      }
      
      MyListener listener = new MyListener();

      session.addFailureListener(listener);

      InVMConnector.failOnCreateConnection = true;
      
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      Thread.sleep(retryInterval * 2);
      
      assertTrue(listener.listenerCalled);

      session.close();

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
