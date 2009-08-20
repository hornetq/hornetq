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
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
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
 * A FailoverExpiredMessageTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 5 Nov 2008 09:33:32
 *
 *
 */
public class FailoverExpiredMessageTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FailoverExpiredMessageTest.class);

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
    * Set messages to expire very soon, send a load of them, so at some of them get expired when they reach the client
    * After failover make sure all are received ok
    */
   public void testExpiredBeforeConsumption() throws Exception
   {            
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));
      
      sf1.setProducerWindowSize(32 * 1024);
  
      ClientSession session1 = sf1.createSession(false, true, true);
      
      session1.createQueue(ADDRESS, ADDRESS, null, false);
       
      session1.start();

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 10000;
      
      //Set time to live so at least some of them will more than likely expire before they are consumed by the client
      
      long now = System.currentTimeMillis();
       
      long expire = now + 5000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             expire,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);         
         message.getBody().writeString("aardvarks");
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
               Thread.sleep(5000);
            }
            catch (InterruptedException e)
            {               
            }
            
            conn1.fail(new HornetQException(HornetQException.NOT_CONNECTED));
         }
      };
      
      t.start();
                   
      int count = 0;
      
      while (true)
      {
         ClientMessage message = consumer1.receive(1000);
                           
         if (message != null)
         {
            message.acknowledge();
            
            //We sleep a little to make sure messages aren't consumed too quickly and some
            //will expire before reaching consumer
            Thread.sleep(1);
            
            count++;
         }
         else
         {
            break;
         }
      }           
      
      t.join();
                   
      session1.close();
      
      //Make sure no more messages
      ClientSession session2 = sf1.createSession(false, true, true);
      
      session2.start();
      
      ClientConsumer consumer2 = session2.createConsumer(ADDRESS);
      
      ClientMessage message = consumer2.receive(1000);
      
      assertNull(message);
      
      session2.close();      
   }
   
   public void testExpireViaReaperOnLive() throws Exception
   {            
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));
      
      sf1.setProducerWindowSize(32 * 1024);
  
      ClientSession session1 = sf1.createSession(false, true, true);
      
      session1.createQueue(ADDRESS, ADDRESS, null, false);
          
      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 10000;
      
      //Set time to live so messages are expired on the server
      
      long now = System.currentTimeMillis();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             now,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);         
         message.getBody().writeString("aardvarks");
         producer.send(message);               
      }

      Thread.sleep(4 * expireScanPeriod);
      
      //Messages should all be expired now
      
      ClientConsumer consumer1 = session1.createConsumer(ADDRESS);
      
      session1.start();
                 
      RemotingConnection conn1 = ((ClientSessionInternal)session1).getConnection();
 
      conn1.fail(new HornetQException(HornetQException.NOT_CONNECTED));
                              
      ClientMessage message = consumer1.receive(1000);
      
      assertNull(message);
                         
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

   private final long expireScanPeriod = 1000;
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setMessageExpiryScanPeriod(expireScanPeriod);
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = HornetQ.newMessagingServer(backupConf, false);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setMessageExpiryScanPeriod(expireScanPeriod);
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams, "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = HornetQ.newMessagingServer(liveConf, false);
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

