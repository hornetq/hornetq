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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionCreateConsumerTest extends ServiceTestBase
{
   private String queueName = "ClientSessionCreateConsumerTestQ";

   public void testCreateConsumer() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName);
         assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerNoQ() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         try
         {
            clientSession.createConsumer(queueName);
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(e.getCode(), MessagingException.QUEUE_DOES_NOT_EXIST);
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
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, "foo=bar");
         assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerWithInvalidFilter() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         try
         {
            clientSession.createConsumer(queueName, "foobar");
            fail("should throw exception");
         }
         catch (MessagingException e)
         {
            assertEquals(e.getCode(), MessagingException.INVALID_FILTER_EXPRESSION);
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
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, null, true);
         assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateConsumerWithOverrides() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.createQueue(queueName, queueName, false);
         ClientConsumer consumer = clientSession.createConsumer(queueName, null, 100, 100, false);
         assertNotNull(consumer);
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

}
