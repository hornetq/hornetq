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
package org.hornetq.tests.unit.core.remoting.impl.netty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.NettyAcceptor;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * A NettyAcceptorFactoryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyAcceptorFactoryTest extends UnitTestCase
{
   public void testCreateAcceptor() throws Exception
   {
      NettyAcceptorFactory factory = new NettyAcceptorFactory();

      Map<String, Object> params = new HashMap<String, Object>();
      BufferHandler handler = new BufferHandler()
      {

         public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
         {
         }
      };

      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {

         public void connectionException(final Object connectionID, final HornetQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final Acceptor acceptor, final Connection connection, final ProtocolType protocol)
         {
         }

         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
         
         
      };

      Acceptor acceptor = factory.createAcceptor(null,
                                                 params,
                                                 handler,
                                                 null,
                                                 listener,
                                                 Executors.newCachedThreadPool(),
                                                 Executors.newScheduledThreadPool(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE));

      Assert.assertTrue(acceptor instanceof NettyAcceptor);
   }
}
