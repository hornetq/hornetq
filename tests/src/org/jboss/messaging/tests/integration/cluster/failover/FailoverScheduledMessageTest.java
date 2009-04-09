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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A FailoverScheduledMessageTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 5 Nov 2008 11:18:51
 *
 */
public class FailoverScheduledMessageTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FailoverScheduledMessageTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingServer liveService;

   private MessagingServer backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /*
    * Send some scheduled messsages on live
    * Let some fire on live
    * Failover
    * Let rest fire on backup
    * Assert no duplicates and all are received ok
    */
   public void testScheduled() throws Exception
   {            
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));
      
      sf1.setSendWindowSize(32 * 1024);
  
      ClientSession session1 = sf1.createSession(false, true, true);

      session1.createQueue(ADDRESS, ADDRESS, null, false);
      
      session1.start();

      ClientProducer producer = session1.createProducer(ADDRESS);
                 
      final int numMessages = 10;
      
      long now = System.currentTimeMillis();
      
      final long delay = 200;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);         
         message.getBody().writeString("aardvarks");
         long deliveryTime = now + delay * i;
         message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, deliveryTime);
         producer.send(message);                
      }
      
      ClientConsumer consumer1 = session1.createConsumer(ADDRESS);
                 
      final RemotingConnection conn1 = ((ClientSessionImpl)session1).getConnection();
 
      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               //Sleep a little while to ensure that some messages are consumed before failover
               Thread.sleep(delay * numMessages / 2);
            }
            catch (InterruptedException e)
            {               
            }
            
            conn1.fail(new MessagingException(MessagingException.NOT_CONNECTED));
         }
      };
      
      t.start();
                   
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(delay * 2);
                           
         assertNotNull(message);
            
         message.acknowledge();
      }      
      
      ClientMessage message = consumer1.receive(delay * 2);
      
      assertNull(message);
      
      t.join();
                   
      session1.close();
      
      //Make sure no more messages
      ClientSession session2 = sf1.createSession(false, true, true);
      
      session2.start();
      
      ClientConsumer consumer2 = session2.createConsumer(ADDRESS);
      
      message = consumer2.receive(1000);
      
      assertNull(message);
      
      session2.close();      
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
      backupService = Messaging.newMessagingServer(backupConf, false);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams, "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = Messaging.newMessagingServer(liveConf, false);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


