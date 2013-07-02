/*
 * Copyright 2013 Red Hat, Inc.
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

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.tests.util.SingleServerTestBase;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class AutoCloseCoreTest extends SingleServerTestBase
{

   @Test
   public void testAutClose() throws Exception
   {
      ServerLocator locatorx;
      ClientSession sessionx;
      ClientSessionFactory factoryx;
      try (ServerLocator locator = createLocator();
           ClientSessionFactory factory = locator.createSessionFactory();
           ClientSession session = factory.createSession(false, false))
      {
         locatorx = locator;
         sessionx = session;
         factoryx = factory;
      }


      assertTrue(locatorx.isClosed());
      assertTrue(sessionx.isClosed());
      assertTrue(factoryx.isClosed());
   }
}
