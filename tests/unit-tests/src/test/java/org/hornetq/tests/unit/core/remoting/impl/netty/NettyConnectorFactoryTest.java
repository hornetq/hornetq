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

package org.hornetq.tests.unit.core.remoting.impl.netty;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.remoting.impl.netty.NettyConnector;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.spi.core.remoting.Connector;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author <a href="mailto:mtaylor@redhat.com">Martyn Taylor</a>
 */

public class NettyConnectorFactoryTest
{
   @Test
   public void testCreateConnectorSetsDefaults()
   {
      NettyConnectorFactory nettyConnectorFactory = new NettyConnectorFactory();

      BufferHandler bufferHandler = new BufferHandler()
      {
         @Override
         public void bufferReceived(Object connectionID, HornetQBuffer buffer)
         {
         }
      };

      ConnectionLifeCycleListener connectionLifeCycleListener = new ConnectionLifeCycleListener()
      {
         @Override
         public void connectionCreated(HornetQComponent component, Connection connection, String protocol)
         {
         }

         @Override
         public void connectionDestroyed(Object connectionID)
         {
         }

         @Override
         public void connectionException(Object connectionID, HornetQException me)
         {
         }

         @Override
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      // Tests defaults are set when config is null
      Connector connector = nettyConnectorFactory.createConnector(null, bufferHandler, connectionLifeCycleListener,
                                                                  null, null, null);
      assertTrue(connector.isEquivalent(NettyConnector.DEFAULT_CONFIG));

      // Tests defaults are set when config is empty
      Map<String, Object> config = new HashMap<String, Object>();
      connector = nettyConnectorFactory.createConnector(config, bufferHandler, connectionLifeCycleListener,
                                                        null, null, null);
      assertTrue(connector.isEquivalent(NettyConnector.DEFAULT_CONFIG));

      // Tests defaults are not set when config has entries
      config.put(TransportConstants.HOST_PROP_NAME, "0.0.0.0");
      connector = nettyConnectorFactory.createConnector(config, bufferHandler, connectionLifeCycleListener,
                                                        null, null, null);
      assertFalse(connector.isEquivalent(NettyConnector.DEFAULT_CONFIG));
   }
}
