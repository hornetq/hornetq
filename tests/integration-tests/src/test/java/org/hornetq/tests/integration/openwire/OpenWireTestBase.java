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
package org.hornetq.tests.integration.openwire;

import java.util.HashMap;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.After;
import org.junit.Before;

public class OpenWireTestBase extends ServiceTestBase
{
   public static final String OWHOST = "localhost";
   public static final int OWPORT = 61616;

   protected HornetQServer server;
   protected boolean realStore = false;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = this.createServer(realStore, true);
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, "61616");
      params.put(TransportConstants.PROTOCOLS_PROP_NAME, "OPENWIRE");
      TransportConfiguration transportConfiguration = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      server.getConfiguration().getAcceptorConfigurations().add(transportConfiguration);
      server.start();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      server.stop();
      super.tearDown();
   }

}
