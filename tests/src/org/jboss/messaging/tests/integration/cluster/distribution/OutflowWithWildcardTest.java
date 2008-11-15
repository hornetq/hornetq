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
 * A OutflowWithWildcardTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 15 Nov 2008 08:19:07
 *
 *
 */
public class OutflowWithWildcardTest extends TestCase
{
   private static final Logger log = Logger.getLogger(OutflowWithWildcardTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service0;

   private MessagingService service1;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testWithWildcard() throws Exception
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
      
      final SimpleString address1 = new SimpleString("cheese.stilton");
      
      final SimpleString address2 = new SimpleString("cheese.wensleydale");
      
      final SimpleString address3 = new SimpleString("wine.shiraz");
      
      final SimpleString address4 = new SimpleString("wine.cabernet");
      
      final SimpleString match1 = new SimpleString("cheese.#");
      
            
      OutflowConfiguration ofconfig = new OutflowConfiguration("outflow1", match1.toString(), null, true, 1, 0, connectors);
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
      
      session0.createQueue(address1, address1, null, false, false, true);
      session0.createQueue(address2, address2, null, false, false, true);
      session0.createQueue(address3, address3, null, false, false, true);
      session0.createQueue(address4, address4, null, false, false, true);     
      
      session1.createQueue(address1, address1, null, false, false, true);
      session1.createQueue(address2, address2, null, false, false, true);
      session1.createQueue(address3, address3, null, false, false, true);
      session1.createQueue(address4, address4, null, false, false, true);     
      
      ClientProducer prod0_1 = session0.createProducer(address1);
      ClientProducer prod0_2 = session0.createProducer(address2);
      ClientProducer prod0_3 = session0.createProducer(address3);
      ClientProducer prod0_4 = session0.createProducer(address4);      
      
      ClientConsumer cons0_1 = session0.createConsumer(address1);
      ClientConsumer cons0_2 = session0.createConsumer(address2);
      ClientConsumer cons0_3 = session0.createConsumer(address3);
      ClientConsumer cons0_4 = session0.createConsumer(address4);
      
      ClientConsumer cons1_1 = session1.createConsumer(address1);
      ClientConsumer cons1_2 = session1.createConsumer(address2);
      ClientConsumer cons1_3 = session1.createConsumer(address3);
      ClientConsumer cons1_4 = session1.createConsumer(address4);
      
      session0.start();
      
      session1.start();
      
      final int numMessages = 100;
      
      final SimpleString propKey = new SimpleString("testkey");
      
      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage message = session0.createClientMessage(false);
         message.putIntProperty(propKey, i);
         message.getBody().flip();
              
         prod0_1.send(message);
      }
      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage message = session0.createClientMessage(false);
         message.putIntProperty(propKey, i);
         message.getBody().flip();
              
         prod0_2.send(message);
      }
      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage message = session0.createClientMessage(false);
         message.putIntProperty(propKey, i);
         message.getBody().flip();
              
         prod0_3.send(message);
      }
      for (int i = 0; i < numMessages; i++)
      {      
         ClientMessage message = session0.createClientMessage(false);
         message.putIntProperty(propKey, i);
         message.getBody().flip();
              
         prod0_4.send(message);
      }
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage rmessage1 = cons0_1.receive(1000);
         
         assertNotNull(rmessage1);
         
         assertEquals(i, rmessage1.getProperty(propKey));
         
         ClientMessage rmessage2 = cons0_2.receive(1000);
         
         assertNotNull(rmessage2);
         
         assertEquals(i, rmessage2.getProperty(propKey));
         
         ClientMessage rmessage3 = cons0_3.receive(1000);
         
         assertNotNull(rmessage3);
         
         assertEquals(i, rmessage3.getProperty(propKey));  
         
         ClientMessage rmessage4 = cons0_4.receive(1000);
         
         assertNotNull(rmessage4);
         
         assertEquals(i, rmessage4.getProperty(propKey));  
      }
      
      ClientMessage rmessage1 = cons0_1.receiveImmediate();
      
      assertNull(rmessage1);
      
      ClientMessage rmessage2 = cons0_2.receiveImmediate();
      
      assertNull(rmessage2);
            
      ClientMessage rmessage3 = cons0_3.receiveImmediate();
      
      assertNull(rmessage3);
      
      ClientMessage rmessage4 = cons0_4.receiveImmediate();
      
      assertNull(rmessage4);
      
      for (int i = 0; i < numMessages; i++)
      {
         rmessage1 = cons1_1.receive(1000);
         
         assertNotNull(rmessage1);
         
         assertEquals(i, rmessage1.getProperty(propKey));
         
         rmessage2 = cons1_2.receive(1000);
         
         assertNotNull(rmessage2);
         
         assertEquals(i, rmessage2.getProperty(propKey));         
      }
      
      rmessage1 = cons1_1.receiveImmediate();
      
      assertNull(rmessage1);
      
      rmessage2 = cons1_2.receiveImmediate();
      
      assertNull(rmessage2);
            
      rmessage3 = cons1_3.receiveImmediate();
      
      assertNull(rmessage3);
      
      rmessage4 = cons1_4.receiveImmediate();
      
      assertNull(rmessage4);
      
      session0.close();
      
      session1.close();
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

