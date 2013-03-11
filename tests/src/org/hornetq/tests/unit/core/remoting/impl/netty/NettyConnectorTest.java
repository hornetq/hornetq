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
import org.hornetq.core.remoting.impl.netty.NettyConnector;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.BufferHandler;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * A MinaConnectorTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyConnectorTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

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

         public void connectionCreated(final Acceptor acceptor, final Connection connection, final ProtocolType protocol)
         {
         }
         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      NettyConnector connector = new NettyConnector(params,
                                                    handler,
                                                    listener,
                                                    Executors.newCachedThreadPool(),
                                                    Executors.newCachedThreadPool(),
                                                    Executors.newScheduledThreadPool(5));

      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }

   public void testNullParams() throws Exception
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

         public void connectionCreated(final Acceptor acceptor, final Connection connection, final ProtocolType protocol)
         {
         }

         public void connectionReadyForWrites(Object connectionID, boolean ready)
         {
         }
      };

      try
      {
         new NettyConnector(params,
                            null,
                            listener,
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool(),
                            Executors.newScheduledThreadPool(5));

         Assert.fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         // Ok
      }

      try
      {
         new NettyConnector(params,
                            handler,
                            null,
                            Executors.newCachedThreadPool(),
                            Executors.newCachedThreadPool(),
                            Executors.newScheduledThreadPool(5));

         Assert.fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         // Ok
      }
   }
   public void testSystemPropertyOverrides() throws Exception
   {
      BufferHandler handler = new BufferHandler()
      {
         public void bufferReceived(final Object connectionID, final HornetQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.SSL_ENABLED_PROP_NAME, true);
      params.put(TransportConstants.KEYSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.KEYSTORE_PASSWORD_PROP_NAME, "bad password");
      params.put(TransportConstants.TRUSTSTORE_PATH_PROP_NAME, "bad path");
      params.put(TransportConstants.TRUSTSTORE_PASSWORD_PROP_NAME, "bad password");
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

      NettyConnector connector = new NettyConnector(params,
            handler,
            listener,
            Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool(),
            Executors.newScheduledThreadPool(5));


      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PATH_PROP_NAME, "client-side.keystore");
      System.setProperty(NettyConnector.JAVAX_KEYSTORE_PASSWORD_PROP_NAME, "secureexample");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PATH_PROP_NAME, "client-side.truststore");
      System.setProperty(NettyConnector.JAVAX_TRUSTSTORE_PASSWORD_PROP_NAME, "secureexample");

      connector.start();
      Assert.assertTrue(connector.isStarted());
      connector.close();
      Assert.assertFalse(connector.isStarted());
   }
}
