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
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQInvalidFilterExpressionException;
import org.hornetq.api.core.HornetQNonExistentQueueException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionCreateConsumerTest extends ServiceTestBase
{
   private final String queueName = "ClientSessionCreateConsumerTestQ";

   private ServerLocator locator;
   private HornetQServer service;
   private ClientSessionInternal clientSession;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      locator = createInVMNonHALocator();
      super.setUp();

      service = createServer(false);
      service.start();
      locator.setProducerMaxRate(99);
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnNonDurableSend(true);
      cf = createSessionFactory(locator);
      clientSession = (ClientSessionInternal)addClientSession(cf.createSession(false, true, true));
   }

   @Test
   public void testCreateConsumer() throws Exception
   {
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName);
         Assert.assertNotNull(consumer);
   }

   @Test
   public void testCreateConsumerNoQ() throws Exception
   {
         try
         {
            clientSession.createConsumer(queueName);
            Assert.fail("should throw exception");
         }
         catch(HornetQNonExistentQueueException neqe)
         {
            //ok
         }
         catch (HornetQException e)
         {
            fail("Invalid Exception type:" + e.getType());
         }
   }

   @Test
   public void testCreateConsumerWithFilter() throws Exception
   {
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, "foo=bar");
         Assert.assertNotNull(consumer);
   }

   @Test
   public void testCreateConsumerWithInvalidFilter() throws Exception
   {
         clientSession.createQueue(queueName, queueName, false);
         try
         {
            clientSession.createConsumer(queueName, "this is not valid filter");
            Assert.fail("should throw exception");
         }
         catch(HornetQInvalidFilterExpressionException ifee)
         {
            //ok
         }
         catch (HornetQException e)
         {
            fail("Invalid Exception type:" + e.getType());
         }
   }

   @Test
   public void testCreateConsumerWithBrowseOnly() throws Exception
   {
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, null, true);
         Assert.assertNotNull(consumer);
   }

   @Test
   public void testCreateConsumerWithOverrides() throws Exception
   {
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, null, 100, 100, false);
         Assert.assertNotNull(consumer);
   }

}
