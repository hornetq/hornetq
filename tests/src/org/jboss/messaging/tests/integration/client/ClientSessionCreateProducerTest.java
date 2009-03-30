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

import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionInternal;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientSessionCreateProducerTest extends ServiceTestBase
{
   public void testCreateAnonProducer() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal) cf.createSession(false, true, true);
         ClientProducer producer = clientSession.createProducer();
         assertNull(producer.getAddress());
         assertEquals(cf.getProducerMaxRate(), producer.getMaxRate());
         assertEquals(cf.isBlockOnNonPersistentSend(),producer.isBlockOnNonPersistentSend());
         assertEquals(cf.isBlockOnPersistentSend(),producer.isBlockOnPersistentSend());
         assertFalse(producer.isClosed());
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateProducer1() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal) cf.createSession(false, true, true);
         ClientProducer producer = clientSession.createProducer("testAddress");
         assertNotNull(producer.getAddress());
         assertEquals(cf.getProducerMaxRate(), producer.getMaxRate());
         assertEquals(cf.isBlockOnNonPersistentSend(),producer.isBlockOnNonPersistentSend());
         assertEquals(cf.isBlockOnPersistentSend(),producer.isBlockOnPersistentSend());
         assertFalse(producer.isClosed());
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateProducer2() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal) cf.createSession(false, true, true);
         int rate = 9876;
         ClientProducer producer = clientSession.createProducer("testAddress", rate);
         assertNotNull(producer.getAddress());
         assertEquals(rate, producer.getMaxRate());
         assertEquals(cf.isBlockOnNonPersistentSend(),producer.isBlockOnNonPersistentSend());
         assertEquals(cf.isBlockOnPersistentSend(),producer.isBlockOnPersistentSend());
         assertFalse(producer.isClosed());
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateProducer3() throws Exception
   {
      MessagingServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal) cf.createSession(false, true, true);
         int rate = 9876;
         boolean blockOnSend = false;
         boolean blockOnNonSend = false;
         ClientProducer producer = clientSession.createProducer("testAddress", 9876, blockOnSend, blockOnNonSend);
         assertNotNull(producer.getAddress());
         assertEquals(rate, producer.getMaxRate());
         assertEquals(blockOnSend, producer.isBlockOnNonPersistentSend());
         assertEquals(blockOnNonSend, producer.isBlockOnPersistentSend());
         assertFalse(producer.isClosed());
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testProducerOnClosedSession() throws Exception
      {
         MessagingServer service = createServer(false);
         try
         {
            service.start();
            ClientSessionFactory cf = createInVMFactory();
            cf.setProducerMaxRate(99);
            cf.setBlockOnNonPersistentSend(true);
            cf.setBlockOnNonPersistentSend(true);
            ClientSessionInternal clientSession = (ClientSessionInternal) cf.createSession(false, true, true);
            clientSession.close();
            try
            {
               clientSession.createProducer();
               fail("should throw exception");
            }
            catch (MessagingException e)
            {
               assertEquals(e.getCode(), MessagingException.OBJECT_CLOSED);
            }
         }
         finally
         {
            service.stop();
         }
      }

}
