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

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A SessionClosedOnRemotingConnectionFailureTest
 *
 * @author Tim Fox
 
 */
public class SessionClosedOnRemotingConnectionFailureTest extends UnitTestCase
{
   private HornetQServer server;

   private ClientSessionFactory sf;

   public void testSessionClosedOnRemotingConnectionFailure() throws Exception
   {
      ClientSession session = null;

      try
      {
         session = sf.createSession();

         session.createQueue("fooaddress", "fooqueue");

         ClientProducer prod = session.createProducer("fooaddress");

         ClientConsumer cons = session.createConsumer("fooqueue");

         session.start();

         prod.send(session.createMessage(false));

         Assert.assertNotNull(cons.receive());

         // Now fail the underlying connection

         RemotingConnection connection = ((ClientSessionInternal)session).getConnection();

         connection.fail(new HornetQException(HornetQException.NOT_CONNECTED));

         Assert.assertTrue(session.isClosed());

         Assert.assertTrue(prod.isClosed());

         Assert.assertTrue(cons.isClosed());

         // Now try and use the producer

         try
         {
            prod.send(session.createMessage(false));

            Assert.fail("Should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
         }

         try
         {
            cons.receive();

            Assert.fail("Should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
         }

         session.close();
      }
      finally
      {
         if (session != null)
         {
            session.close();
         }
      }
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration config = new ConfigurationImpl();
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);
      server = HornetQServers.newHornetQServer(config, false);

      server.start();
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      sf = locator.createSessionFactory();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (sf != null)
      {
         sf.close();
      }

      if (server != null)
      {
         server.stop();
      }

      sf = null;

      server = null;

      super.tearDown();
   }
}
