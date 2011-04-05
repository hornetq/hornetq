/*
 * Copyright 2010 Red Hat, Inc.
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
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionProducerCreditsMessage;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A FailoverOnFlowControlTest
 *
 * @author clebertsuconic
 *
 *
 */
public class FailoverOnFlowControlTest extends FailoverTestBase
{


   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   
   public void testOverflowSend() throws Exception
   {
      ServerLocator locator = getServerLocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);
      locator.setProducerWindowSize(1000);
      final ArrayList<ClientSession> sessionList = new ArrayList<ClientSession>();
      Interceptor interceptorClient = new Interceptor()
      {
         AtomicInteger count = new AtomicInteger(0);
         public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
         {
            System.out.println("Intercept..." + packet.getClass().getName());
            
            if (packet instanceof SessionProducerCreditsMessage )
            {
               SessionProducerCreditsMessage credit = (SessionProducerCreditsMessage)packet;
               
               System.out.println("Credits: " + credit.getCredits());
               if (count.incrementAndGet() == 2)
               {
                  try
                  {
                     crash(sessionList.get(0));
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
                  return false;
               }
            }
            return true;
         }
      };
      
      locator.addInterceptor(interceptorClient);

      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      ClientSession session = sf.createSession(true, true);
      sessionList.add(session);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);
      

      final int numMessages = 10;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(true);
         
         message.getBodyBuffer().writeBytes(new byte[5000]);

         message.putIntProperty("counter", i);

         producer.send(message);
      }
      
      session.close();
      
      locator.close();
   }


   protected void createConfigs() throws Exception
   {
      super.createConfigs();
      liveServer.getServer().getConfiguration().setJournalFileSize(1024 * 1024);
      backupServer.getServer().getConfiguration().setJournalFileSize(1024 * 1024);
   }

   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocatorInternal locator = super.getServerLocator();
      locator.setMinLargeMessageSize(1024 * 1024);
      locator.setProducerWindowSize(10 * 1024);
      return locator;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return getInVMTransportAcceptorConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return getInVMConnectorTransportConfiguration(live);
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
