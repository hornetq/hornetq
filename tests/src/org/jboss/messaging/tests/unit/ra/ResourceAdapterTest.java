/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.unit.ra;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Timer;

import javax.jms.Connection;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.UnavailableException;
import javax.resource.spi.XATerminator;
import javax.resource.spi.endpoint.MessageEndpoint;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.ra.JBMManagedConnectionFactory;
import org.jboss.messaging.ra.JBMResourceAdapter;
import org.jboss.messaging.ra.inflow.JBMActivation;
import org.jboss.messaging.ra.inflow.JBMActivationSpec;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * A ResourceAdapterTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ResourceAdapterTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testValidateProperties() throws Exception
   {
      validateGettersAndSetters(new JBMResourceAdapter(), "backupTransportConfiguration");
      validateGettersAndSetters(new JBMManagedConnectionFactory(), "connectionParameters", "sessionDefaultType");
      validateGettersAndSetters(new JBMActivationSpec(),
                                "connectionParameters",
                                "acknowledgeMode",
                                "subscriptionDurability");

      JBMActivationSpec spec = new JBMActivationSpec();

      spec.setAcknowledgeMode("DUPS_OK_ACKNOWLEDGE");
      spec.setSessionTransacted(false);
      assertEquals("Dups-ok-acknowledge", spec.getAcknowledgeMode());

      spec.setSessionTransacted(true);

      assertEquals("Transacted", spec.getAcknowledgeMode());

      spec.setSubscriptionDurability("Durable");
      assertEquals("Durable", spec.getSubscriptionDurability());

      spec.setSubscriptionDurability("NonDurable");
      assertEquals("NonDurable", spec.getSubscriptionDurability());
      
      
      spec = new JBMActivationSpec();
      JBMResourceAdapter adapter = new JBMResourceAdapter();

      adapter.setUserName("us1");
      adapter.setPassword("ps1");
      adapter.setClientID("cl1");
      
      spec.setResourceAdapter(adapter);
      
      assertEquals("us1", spec.getUser());
      assertEquals("ps1", spec.getPassword());
      assertEquals("cl1", spec.getClientId());
      
      spec.setUser("us2");
      spec.setPassword("ps2");
      spec.setClientId("cl2");

      
      assertEquals("us2", spec.getUser());
      assertEquals("ps2", spec.getPassword());
      assertEquals("cl2", spec.getClientId());
      
      
   }

   public void testStartActivation() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {

         server.start();
         
         ClientSessionFactory factory = createInVMFactory();
         ClientSession session = factory.createSession(false, false, false);
         JBossQueue queue = new JBossQueue("test");
         session.createQueue(queue.getSimpleAddress(), queue.getSimpleAddress(), true);
         session.close();
         
         JBMResourceAdapter ra = new JBMResourceAdapter();
         ra.setConnectorClassName("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory");
         ra.setConnectionParameters("bm.remoting.invm.serverid=0");
         ra.setUseXA(true);
         ra.setUserName("userGlobal");
         ra.setPassword("passwordGlobal");
         ra.start(fakeCTX);

         Connection conn = ra.getJBossConnectionFactory().createConnection();
         
         conn.close();
         
         JBMActivationSpec spec = new JBMActivationSpec();
         
         spec.setResourceAdapter(ra);
         
         spec.setUseJNDI(false);
         
         spec.setUser("user");
         spec.setPassword("password");
         
         spec.setDestinationType("Topic");
         spec.setDestination("test");

         spec.setMinSession(1);
         spec.setMaxSession(1);
         
         JBMActivation activation = new JBMActivation(ra, new FakeMessageEndpointFactory(), spec);
         
         activation.start();
         activation.stop();
         
      }
      finally
      {
         server.stop();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class MockJBMResourceAdapter extends JBMResourceAdapter
   {
      public JBossConnectionFactory createRemoteFactory(String connectorClassName,
                                                        Map<String, Object> connectionParameters)
      {
         JBossConnectionFactory factory = super.createRemoteFactory(connectorClassName, connectionParameters);

         return factory;
      }
   }

   BootstrapContext fakeCTX = new BootstrapContext()
   {

      public Timer createTimer() throws UnavailableException
      {
         return null;
      }

      public WorkManager getWorkManager()
      {
         return null;
      }

      public XATerminator getXATerminator()
      {
         return null;
      }

   };
   
   
   class FakeMessageEndpointFactory implements MessageEndpointFactory
   {

      /* (non-Javadoc)
       * @see javax.resource.spi.endpoint.MessageEndpointFactory#createEndpoint(javax.transaction.xa.XAResource)
       */
      public MessageEndpoint createEndpoint(XAResource arg0) throws UnavailableException
      {
         return null;
      }

      /* (non-Javadoc)
       * @see javax.resource.spi.endpoint.MessageEndpointFactory#isDeliveryTransacted(java.lang.reflect.Method)
       */
      public boolean isDeliveryTransacted(Method arg0) throws NoSuchMethodException
      {
         return false;
      }
      
   }
}
