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

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.integration.transports.netty.NettyConnection;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.spi.core.remoting.ConnectionLifeCycleListener;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelConfig;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;

/**
 *
 * A NettyConnectionTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NettyConnectionTest extends UnitTestCase
{

   public void testGetID() throws Exception
   {
      Channel channel = new SimpleChannel(RandomUtil.randomInt());
      NettyConnection conn = new NettyConnection(channel, new MyListener());

      Assert.assertEquals(channel.getId().intValue(), conn.getID());
   }

   public void testWrite() throws Exception
   {
      HornetQBuffer buff = HornetQBuffers.wrappedBuffer(ByteBuffer.allocate(128));
      SimpleChannel channel = new SimpleChannel(RandomUtil.randomInt());

      Assert.assertEquals(0, channel.getWritten().size());

      NettyConnection conn = new NettyConnection(channel, new MyListener());
      conn.write(buff);

      Assert.assertEquals(1, channel.getWritten().size());
   }

   public void testCreateBuffer() throws Exception
   {
      Channel channel = new SimpleChannel(RandomUtil.randomInt());
      NettyConnection conn = new NettyConnection(channel, new MyListener());

      final int size = 1234;

      HornetQBuffer buff = conn.createBuffer(size);
      buff.writeByte((byte)0x00); // Netty buffer does lazy initialization.
      Assert.assertEquals(size, buff.capacity());

   }

   private final class SimpleChannel implements Channel
   {
      private final int id;

      private final List<Object> written = new LinkedList<Object>();

      private SimpleChannel(final int id)
      {
         this.id = id;
      }

      public List<Object> getWritten()
      {
         return written;
      }

      public int compareTo(final Channel arg0)
      {
         return 0;
      }

      public ChannelFuture write(final Object arg0, final SocketAddress arg1)
      {
         written.add(arg0);
         return null;
      }

      public ChannelFuture write(final Object arg0)
      {
         written.add(arg0);
         return null;
      }

      public ChannelFuture unbind()
      {
         return null;
      }

      public ChannelFuture setReadable(final boolean arg0)
      {
         return null;
      }

      public ChannelFuture setInterestOps(final int arg0)
      {
         return null;
      }

      public boolean isWritable()
      {
         return false;
      }

      public boolean isReadable()
      {
         return false;
      }

      public boolean isOpen()
      {
         return false;
      }

      public boolean isConnected()
      {
         return false;
      }

      public boolean isBound()
      {
         return false;
      }

      public SocketAddress getRemoteAddress()
      {
         return null;
      }

      public ChannelPipeline getPipeline()
      {
         return null;
      }

      public Channel getParent()
      {
         return null;
      }

      public SocketAddress getLocalAddress()
      {
         return null;
      }

      public int getInterestOps()
      {
         return 0;
      }

      public Integer getId()
      {
         return id;
      }

      public ChannelFactory getFactory()
      {
         return null;
      }

      public ChannelConfig getConfig()
      {
         return null;
      }

      public ChannelFuture getCloseFuture()
      {
         return null;
      }

      public ChannelFuture disconnect()
      {
         return null;
      }

      public ChannelFuture connect(final SocketAddress arg0)
      {
         return null;
      }

      public ChannelFuture close()
      {
         return null;
      }

      public ChannelFuture bind(final SocketAddress arg0)
      {
         return null;
      }
   }

   class MyListener implements ConnectionLifeCycleListener
   {

      public void connectionCreated(final Connection connection, final ProtocolType protocol)
      {

      }

      public void connectionDestroyed(final Object connectionID)
      {

      }

      public void connectionException(final Object connectionID, final HornetQException me)
      {

      }

   }
}
