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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.impl.AbstractBufferHandler;
import org.hornetq.core.remoting.spi.BufferHandler;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.integration.transports.netty.NettyConnector;
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
      BufferHandler handler = new AbstractBufferHandler()
      {
         public void bufferReceived(Object connectionID, HornetQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(Object connectionID, HornetQException me)
         {
         }
         
         public void connectionDestroyed(Object connectionID)
         {
         }
         
         public void connectionCreated(Connection connection)
         {
         }
      };
      
      NettyConnector connector = new NettyConnector(params, handler, listener, Executors.newCachedThreadPool(), Executors.newScheduledThreadPool(5));
      
      connector.start();
      assertTrue(connector.isStarted());
      connector.close();
      assertFalse(connector.isStarted());
   }
   
   public void testNullParams() throws Exception
   {
      BufferHandler handler = new AbstractBufferHandler()
      {
         public void bufferReceived(Object connectionID, HornetQBuffer buffer)
         {
         }
      };
      Map<String, Object> params = new HashMap<String, Object>();
      ConnectionLifeCycleListener listener = new ConnectionLifeCycleListener()
      {
         public void connectionException(Object connectionID, HornetQException me)
         {
         }
         
         public void connectionDestroyed(Object connectionID)
         {
         }
         
         public void connectionCreated(Connection connection)
         {
         }
      };

      try
      {
         new NettyConnector(params, null, listener, Executors.newCachedThreadPool(), Executors.newScheduledThreadPool(5));
         
         fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }
      
      try
      {
         new NettyConnector(params, handler, null, Executors.newCachedThreadPool(), Executors.newScheduledThreadPool(5));
         
         fail("Should throw Exception");
      }
      catch (IllegalArgumentException e)
      {
         //Ok
      }
   }
}
