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

import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SessionCreateProducerTest extends ServiceTestBase
{
   public void testCreateAnonProducer() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         ClientProducer producer = clientSession.createProducer();
         Assert.assertNull(producer.getAddress());
         Assert.assertEquals(cf.getProducerMaxRate(), producer.getMaxRate());
         Assert.assertEquals(cf.isBlockOnNonPersistentSend(), producer.isBlockOnNonPersistentSend());
         Assert.assertEquals(cf.isBlockOnPersistentSend(), producer.isBlockOnPersistentSend());
         Assert.assertFalse(producer.isClosed());
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testCreateProducer1() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         ClientProducer producer = clientSession.createProducer("testAddress");
         Assert.assertNotNull(producer.getAddress());
         Assert.assertEquals(cf.getProducerMaxRate(), producer.getMaxRate());
         Assert.assertEquals(cf.isBlockOnNonPersistentSend(), producer.isBlockOnNonPersistentSend());
         Assert.assertEquals(cf.isBlockOnPersistentSend(), producer.isBlockOnPersistentSend());
         Assert.assertFalse(producer.isClosed());
         clientSession.close();
      }
      finally
      {
         service.stop();
      }
   }

   public void testProducerOnClosedSession() throws Exception
   {
      HornetQServer service = createServer(false);
      try
      {
         service.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setProducerMaxRate(99);
         cf.setBlockOnNonPersistentSend(true);
         cf.setBlockOnNonPersistentSend(true);
         ClientSessionInternal clientSession = (ClientSessionInternal)cf.createSession(false, true, true);
         clientSession.close();
         try
         {
            clientSession.createProducer();
            Assert.fail("should throw exception");
         }
         catch (HornetQException e)
         {
            Assert.assertEquals(e.getCode(), HornetQException.OBJECT_CLOSED);
         }
      }
      finally
      {
         service.stop();
      }
   }

}
