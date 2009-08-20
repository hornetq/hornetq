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

package org.hornetq.tests.integration.management;

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashMap;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.AcceptorControl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class AcceptorControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(AcceptorControlTest.class);

   
   // Attributes ----------------------------------------------------

   private HornetQServer service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         randomString());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = HornetQ.newMessagingServer(conf, mbeanServer, false);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      assertEquals(acceptorConfig.getName(), acceptorControl.getName());
      assertEquals(acceptorConfig.getFactoryClassName(), acceptorControl.getFactoryClassName());
   }

   public void testStartStop() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         randomString());
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = HornetQ.newMessagingServer(conf, mbeanServer, false);
      service.start();

      AcceptorControl acceptorControl = createManagementControl(acceptorConfig.getName());

      // started by the server
      assertTrue(acceptorControl.isStarted());

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      ClientSession session = sf.createSession(false, true, true);
      assertNotNull(session);
      session.close();
      
            
      acceptorControl.stop();
       
      assertFalse(acceptorControl.isStarted());
      
      try
      {
         sf.createSession(false, true, true);
         fail("acceptor must not accept connections when stopped accepting");
      }
      catch (Exception e)
      {
      }
      
      acceptorControl.start();

      assertTrue(acceptorControl.isStarted());
      session = sf.createSession(false, true, true);
      assertNotNull(session);
      session.close();
      
      acceptorControl.stop();
      
      assertFalse(acceptorControl.isStarted());
      
      try
      {
         sf.createSession(false, true, true);
         fail("acceptor must not accept connections when stopped accepting");
      }
      catch (Exception e)
      {
      }
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      if (service != null)
      {
         service.stop();
      }

      super.tearDown();
   }
   
   protected AcceptorControl createManagementControl(String name) throws Exception
   {
      return ManagementControlHelper.createAcceptorControl(name, mbeanServer);
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
