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
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * A NettyAcceptorTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyAcceptorTest extends UnitTestCase
{
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      UnitTestCase.checkFreePort(TransportConstants.DEFAULT_PORT);
   }

   @Override
   protected void tearDown() throws Exception
   {
      UnitTestCase.checkFreePort(TransportConstants.DEFAULT_PORT);

      super.tearDown();
   }

   public void testStartStop() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {

         public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
         {
         }
      };

      Map<String, Object> params = new HashMap<String, Object>();
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {

         public void connectionException(final Object connectionID, final HornetQException me)
         {
         }

         public void connectionDestroyed(final Object connectionID)
         {
         }

         public void connectionCreated(final Connection connection, final ProtocolType protocol)
         {
         }
      };
      NettyAcceptor acceptor = new NettyAcceptor(params,
                                                 handler,
                                                 null,
                                                 listener,
                                                 Executors.newCachedThreadPool(),
                                                 Executors.newScheduledThreadPool(ConfigurationImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE));

      acceptor.start();
      Assert.assertTrue(acceptor.isStarted());
      acceptor.stop();
      Assert.assertFalse(acceptor.isStarted());
      UnitTestCase.checkFreePort(TransportConstants.DEFAULT_PORT);

      acceptor.start();
      Assert.assertTrue(acceptor.isStarted());
      acceptor.stop();
      Assert.assertFalse(acceptor.isStarted());
      UnitTestCase.checkFreePort(TransportConstants.DEFAULT_PORT);
   }

}
