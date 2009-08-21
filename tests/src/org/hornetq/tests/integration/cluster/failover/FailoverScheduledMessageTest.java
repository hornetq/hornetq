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

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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

   private HornetQServer liveService;

   private HornetQServer backupService;

   private Map<String, Object> backupParams = new HashMap<String, Object>();

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
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));
      
      sf1.setProducerWindowSize(32 * 1024);
  
      ClientSession session1 = sf1.createSession(false, true, true);

      session1.createQueue(ADDRESS, ADDRESS, null, false);
      
      session1.start();

      ClientProducer producer = session1.createProducer(ADDRESS);
                 
      final int numMessages = 10;
      
      long now = System.currentTimeMillis();
      
      final long delay = 200;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(HornetQTextMessage.TYPE,
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
                 
      final RemotingConnection conn1 = ((ClientSessionInternal)session1).getConnection();
 
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
            
            conn1.fail(new HornetQException(HornetQException.NOT_CONNECTED));
         }
      };
      
      t.start();

      ClientSession session2 = null;
      try
      {
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
         session2 = sf1.createSession(false, true, true);

         session2.start();

         ClientConsumer consumer2 = session2.createConsumer(ADDRESS);

         message = consumer2.receive(1000);

         assertNull(message);

         session2.close(); 
      }
      finally 
      {
         if (session1 != null)
         {
            session1.close();
         }
         if (session2 != null)
         {
            session2.close();
         }
      }
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
                                                                   backupParams, "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = HornetQ.newHornetQServer(liveConf, false);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
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


