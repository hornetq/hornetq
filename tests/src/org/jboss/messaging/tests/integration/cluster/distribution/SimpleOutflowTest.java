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

package org.jboss.messaging.tests.integration.cluster.distribution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.OutflowConfiguration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ActivationTimeoutTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class SimpleOutflowTest extends TestCase
{
   private static final Logger log = Logger.getLogger(SimpleOutflowTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service0;

   private MessagingService service1;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSimpleOutflow() throws Exception
   {
      Configuration service0Conf = new ConfigurationImpl();
      service0Conf.setSecurityEnabled(false);
      Map<String, Object> service0Params = new HashMap<String, Object>();
      service0Params.put(TransportConstants.SERVER_ID_PROP_NAME, 0);
      service0Conf.getAcceptorConfigurations()
                  .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                  service0Params));

      Configuration service1Conf = new ConfigurationImpl();
      service1Conf.setSecurityEnabled(false);
      Map<String, Object> service1Params = new HashMap<String, Object>();
      service1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

      service1Conf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                                              service1Params));
      service1 = MessagingServiceImpl.newNullStorageMessagingServer(service1Conf);
      service1.start();

      List<TransportConfiguration> connectors = new ArrayList<TransportConfiguration>();
      TransportConfiguration server1tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service1Params);
      connectors.add(server1tc);
      
      final SimpleString testAddress = new SimpleString("testaddress");

      OutflowConfiguration ofconfig = new OutflowConfiguration("outflow1", testAddress.toString(), null, true, 1, 0, connectors);
      Set<OutflowConfiguration> ofconfigs = new HashSet<OutflowConfiguration>();
      ofconfigs.add(ofconfig);
      service0Conf.setOutFlowConfigurations(ofconfigs);

      service0 = MessagingServiceImpl.newNullStorageMessagingServer(service0Conf);
      service0.start();
      
      TransportConfiguration server0tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service0Params);
      
      ClientSessionFactory csf0 = new ClientSessionFactoryImpl(server0tc);
      
      ClientSession session0 = csf0.createSession(false, true, true);
      
      ClientSessionFactory csf1 = new ClientSessionFactoryImpl(server1tc);
      
      ClientSession session1 = csf1.createSession(false, true, true);
      
      session0.createQueue(testAddress, testAddress, null, false, false, true);
      
      session1.createQueue(testAddress, testAddress, null, false, false, true);
      
      ClientProducer prod0 = session0.createProducer(testAddress);
      
      ClientConsumer cons0 = session0.createConsumer(testAddress);
      
      ClientConsumer cons1 = session1.createConsumer(testAddress);
      
      session0.start();
      
      session1.start();
      
      SimpleString propKey = new SimpleString("hello");
      SimpleString propVal = new SimpleString("world");
      
      ClientMessage message = session0.createClientMessage(false);
      message.putStringProperty(propKey, propVal);
      message.getBody().flip();
           
      prod0.send(message);
      
      ClientMessage rmessage0 = cons0.receive(1000);
      
      assertNotNull(rmessage0);
      
      assertEquals(propVal, rmessage0.getProperty(propKey));
      
      ClientMessage rmessage1 = cons1.receive(1000);
      
      assertNotNull(rmessage1);
      
      assertEquals(propVal, rmessage1.getProperty(propKey));
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
   }

   @Override
   protected void tearDown() throws Exception
   {
      service0.stop();
      
      assertEquals(0, service0.getServer().getRemotingService().getConnections().size());

      service1.stop();

      assertEquals(0, service1.getServer().getRemotingService().getConnections().size());

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
