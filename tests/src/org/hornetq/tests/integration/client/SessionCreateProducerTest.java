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

import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionCreateProducerTest extends ServiceTestBase
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
