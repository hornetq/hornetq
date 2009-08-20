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
package org.hornetq.tests.integration.client;

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.Messaging;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ProducerCloseTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCanNotUseAClosedProducer() throws Exception
   {
      final ClientProducer producer = session.createProducer(randomSimpleString());

      assertFalse(producer.isClosed());

      producer.close();

      assertTrue(producer.isClosed());

      expectMessagingException(MessagingException.OBJECT_CLOSED, new MessagingAction()
      {
         public void run() throws MessagingException
         {
            producer.send(session.createClientMessage(false));
         }
      });
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      Configuration config = new ConfigurationImpl();
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);
      server = Messaging.newMessagingServer(config, false);
      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
   }
   
   private ClientSessionFactory sf;

   @Override
   protected void tearDown() throws Exception
   {
      session.close();
            
      sf.close();

      server.stop();
      
      server = null;
      
      session = null;      

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
