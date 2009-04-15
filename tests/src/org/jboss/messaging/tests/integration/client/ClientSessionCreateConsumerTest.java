/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientSessionCreateConsumerTest extends ServiceTestBase
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
