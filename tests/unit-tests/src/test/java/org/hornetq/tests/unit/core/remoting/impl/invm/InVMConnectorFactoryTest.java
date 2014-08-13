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

package org.hornetq.tests.unit.core.remoting.impl.invm;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.impl.netty.NettyConnector;
import org.hornetq.spi.core.remoting.Connector;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class InVMConnectorFactoryTest
{
   @Test
   public void testCreateConnectorSetsDefaults()
   {
      InVMConnectorFactory inVMConnectorFactory = new InVMConnectorFactory();

      // Tests defaults are set when config is null
      Connector connector = inVMConnectorFactory.createConnector(null, null, null, null, null, null);
      assertTrue(connector.isEquivalent(NettyConnector.DEFAULT_CONFIG));

      // Tests defaults are set when config is empty
      Map<String, Object> config = new HashMap<String, Object>();
      connector = inVMConnectorFactory.createConnector(config, null, null, null, null, null);
      assertTrue(connector.isEquivalent(NettyConnector.DEFAULT_CONFIG));

      // Tests defaults are not set when config has entries
      config.put(TransportConstants.SERVER_ID_PROP_NAME, -1);
      connector = inVMConnectorFactory.createConnector(config, null, null, null, null, null);
      assertFalse(connector.isEquivalent(NettyConnector.DEFAULT_CONFIG));
   }
}
