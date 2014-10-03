/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Before;

/**
 * @author Clebert Suconic
 */

public class OrphanedConsumerTest extends ServiceTestBase
{

   private HornetQServer server;

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = createNettyNonHALocator();
   }


   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }


   public void testSimple(final boolean persistent) throws Exception
   {
      server = createServer(persistent, true);
      server.start();

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue("queue", "queue", true);

      ClientProducer prod = session.createProducer("queue");



   }


}