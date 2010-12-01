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

   @Override
   protected void setUp() throws Exception
   {
      locator = createInVMNonHALocator();
      
      super.setUp();   
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();
      
      super.tearDown();
   }

   public void testCreateConsumer() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         locator.setProducerMaxRate(99);
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnNonDurableSend(true);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName);
         Assert.assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerNoQ() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         locator.setProducerMaxRate(99);
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnNonDurableSend(true);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         try
         {
            clientSession.createConsumer(queueName);
            Assert.fail("should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(e.getCode(), HornetQException.QUEUE_DOES_NOT_EXIST);
         }
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerWithFilter() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         locator.setProducerMaxRate(99);
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnNonDurableSend(true);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, "foo=bar");
         Assert.assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerWithInvalidFilter() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         locator.setProducerMaxRate(99);
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnNonDurableSend(true);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         try
         {
            clientSession.createConsumer(queueName, "this is not valid filter");
            Assert.fail("should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(e.getCode(), HornetQException.INVALID_FILTER_EXPRESSION);
         }
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerWithBrowseOnly() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         locator.setProducerMaxRate(99);
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnNonDurableSend(true);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, null, true);
         Assert.assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerWithOverrides() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         locator.setProducerMaxRate(99);
         locator.setBlockOnNonDurableSend(true);
         locator.setBlockOnNonDurableSend(true);
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, null, 100, 100, false);
         Assert.assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

}
