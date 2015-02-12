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
package org.hornetq.tests.performance.netty;

import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Connection;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.ReusableLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author clebertsuconic
 */
@RunWith(Parameterized.class)
public class NettyDirectTest2
{


   @Parameterized.Parameters(name = "useExecutorOption={0}/{1}")
   public static Collection<Object[]> parameters()
   {
      List<Object[]> list = Arrays.asList(new Object[][]{
         {"NoExecutor", 0},
         {"OrderedExecutor", 1},
         {"ServiceExecutor", 2}});
      return list;
   }

   public static void forceGC()
   {
      System.out.println("#test forceGC");
      WeakReference<Object> dumbReference = new WeakReference<Object>(new Object());
      // A loop that will wait GC, using the minimal time as possible
      while (dumbReference.get() != null)
      {
         System.gc();
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
         }
      }
      System.out.println("#test forceGC Done");
   }

   public NettyDirectTest2(String name, int executorOption)
   {
      this.executorOption = executorOption;
      this.testName = name;
   }

   private String testName;
   private final int executorOption;

   ReusableLatch latch = new ReusableLatch(0);

   Channel serverCommunicatingChannel;

   Channel serverChannel;


   ExecutorService executorService = Executors.newCachedThreadPool(new TFactory());
   OrderedExecutorFactory factory = new OrderedExecutorFactory(executorService);
   Executor executor;

   @Before
   public void setup()
   {
      forceGC();
   }

   @After
   public void tearDown()
   {
      try
      {
         final CountDownLatch latch = new CountDownLatch(1);
         executor.execute(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });
         latch.await(1, TimeUnit.SECONDS);
         executorService.shutdownNow();
      }
      catch (Throwable e)
      {
         e.printStackTrace();
      }
      serverChannel.close();
   }

   @Test
   public void testSendReceiveNetty() throws Throwable
   {

      executor = factory.getExecutor();

      startAcceptor();
      Bootstrap bootstrapConnector;

      NioEventLoopGroup group = new NioEventLoopGroup(5);

      bootstrapConnector = new Bootstrap();
      bootstrapConnector.channel(NioSocketChannel.class);
      bootstrapConnector.group(group);
      bootstrapConnector.option(ChannelOption.TCP_NODELAY, true);
//N
//      bootstrapConnector.option(ChannelOption.SO_KEEPALIVE, true);
//      bootstrapConnector.option(ChannelOption.SO_REUSEADDR, true);
      bootstrapConnector.option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));


      bootstrapConnector.handler(new ChannelInitializer<Channel>()
      {
         public void initChannel(Channel channel) throws Exception
         {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new FrameExtractor());
            pipeline.addLast(new ClientReceiver());

         }
      });

      InetSocketAddress socketAddress = new InetSocketAddress("localhost", 1234);

      ChannelFuture future = bootstrapConnector.connect(socketAddress);
      future.await(5, TimeUnit.SECONDS);

      FakeConnection fake = new FakeConnection();
      if (!future.isSuccess())
      {
         throw future.cause();
      }
      else
      {
         int HEAT = 10000;
         int TOTAL_TIME = 50000;
         long time = System.currentTimeMillis();
         long timeStart = System.currentTimeMillis() + HEAT;
         Channel channel = future.channel();
         int initCount = 0;
         long max = Long.MAX_VALUE;
         int count = 0;
         while (System.currentTimeMillis() < max)
         {
            count++;
            if (initCount == 0 && System.currentTimeMillis() > timeStart)
            {
               System.out.println("heat up gone");
               initCount = count;
               max = System.currentTimeMillis() + TOTAL_TIME;
               time = System.currentTimeMillis();
            }

            PacketImpl packet = new PacketImpl(PacketImpl.SESS_COMMIT);
            packet.setChannelID(33L);
            HornetQBuffer buffer = packet.encode(fake);

            latch.countUp();
            channel.writeAndFlush(buffer.byteBuf());
            latch.await();
         }

         printCommitsSecond(time, (count - initCount));

         channel.close().syncUninterruptibly();
      }

   }

   protected void printCommitsSecond(final long start, final double committs)
   {

      long end = System.currentTimeMillis();
      double elapsed = ((double) end - (double) start) / 1000f;

      double commitsPerSecond = committs / elapsed;

      System.out.println("use Executor=" + executorOption + "/" + testName + ", end = " + end + ", start=" + start + ", numberOfMessages="
                            + committs + ", elapsed=" + elapsed + " msgs/sec= " + commitsPerSecond);

   }


   protected void startAcceptor()
   {
      NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup(20, new TFactory());


      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(eventLoopGroup);
      bootstrap.channel(NioServerSocketChannel.class);
      bootstrap.option(ChannelOption.ALLOCATOR, new PooledByteBufAllocator(true));

      InetSocketAddress socketAddress = new InetSocketAddress("localhost", 1234);

      ChannelInitializer<Channel> factory = new ChannelInitializer<Channel>()
      {
         @Override
         public void initChannel(Channel channel) throws Exception
         {
            serverCommunicatingChannel = channel;
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new FrameExtractor());
            pipeline.addLast(new ServerHandler());


         }
      };
      bootstrap.childHandler(factory);


      serverChannel = bootstrap.bind(socketAddress).syncUninterruptibly().channel();

   }

   class ClientReceiver extends ChannelDuplexHandler
   {

      protected ClientReceiver()
      {
      }

      /**
       * Calls {@link io.netty.channel.ChannelHandlerContext#fireExceptionCaught(Throwable)} to forward
       * to the next {@link io.netty.channel.ChannelHandler} in the {@link io.netty.channel.ChannelPipeline}.
       * <p/>
       * Sub-classes may override this method to change behavior.
       */
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
         throws Exception
      {
         cause.printStackTrace();
         ctx.fireExceptionCaught(cause);
      }

      @Override
      public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception
      {
         ByteBuf in = (ByteBuf) msg;

         in.skipBytes(1);
         PacketImpl packet = new PacketImpl(PacketImpl.NULL_RESPONSE);
         HornetQBuffer bufferWrapper = new ChannelBufferWrapper(in, true);
         packet.decode(bufferWrapper);

         in.release();

         latch.countDown();

      }

   }

   class FrameExtractor extends LengthFieldBasedFrameDecoder
   {
      public FrameExtractor()
      {
         super(Integer.MAX_VALUE, 0, DataConstants.SIZE_INT);
      }

      @Override
      protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length)
      {
         return super.extractFrame(ctx, buffer, index, length).skipBytes(DataConstants.SIZE_INT);
      }
   }

   class TFactory implements ThreadFactory
   {
      private final ThreadGroup group;

      private final AtomicInteger threadCount = new AtomicInteger(0);

      private final ClassLoader tccl;

      public TFactory()
      {
         group = new ThreadGroup("TestGroupNetty");

         this.tccl = NettyDirectTest2.this.getClass().getClassLoader();
      }

      public Thread newThread(final Runnable command)
      {
         final Thread t = new Thread(group, command, "TestThread-" + threadCount.getAndIncrement() + " (" + group.getName() + ")");
         t.setContextClassLoader(tccl);
//         t.setPriority(1);

         return t;
      }
   }

   public class ServerHandler extends ByteToMessageDecoder
   {
      public ServerHandler()
      {
      }

      FakeConnection fake = new FakeConnection();

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
      {
         super.channelRead(ctx, msg);
      }

      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
      {
         in.skipBytes(1);
         HornetQBuffer buffer = new ChannelBufferWrapper(in, true);
         PacketImpl packet = new PacketImpl(PacketImpl.SESS_COMMIT);
         packet.decode(buffer);

         final PacketImpl response = new PacketImpl(PacketImpl.NULL_RESPONSE);
         response.setChannelID(33L);

         final HornetQBuffer responseBuffer = response.encode(fake);


         if (executorOption == 0)
         {
            respond(responseBuffer);
         }
         else if (executorOption == 1)
         {
            executor.execute(new Runnable()
            {
               public void run()
               {
                  respond(responseBuffer);
               }
            });
            ctx.flush();
         }
         else
         {
            executorService.execute(new Runnable()
            {
               public void run()
               {
                  respond(responseBuffer);
               }
            });
            ctx.flush();
         }
      }

      private void respond(HornetQBuffer responseBuffer)
      {
         serverCommunicatingChannel.writeAndFlush(responseBuffer.byteBuf());
      }

   }


   class FakeConnection implements RemotingConnection
   {
      @Override
      public Object getID()
      {
         return null;
      }

      @Override
      public long getCreationTime()
      {
         return 0;
      }

      @Override
      public String getRemoteAddress()
      {
         return null;
      }

      @Override
      public void addFailureListener(FailureListener listener)
      {

      }

      @Override
      public boolean removeFailureListener(FailureListener listener)
      {
         return false;
      }

      @Override
      public void addCloseListener(CloseListener listener)
      {

      }

      @Override
      public boolean removeCloseListener(CloseListener listener)
      {
         return false;
      }

      @Override
      public List<CloseListener> removeCloseListeners()
      {
         return null;
      }

      @Override
      public void setCloseListeners(List<CloseListener> listeners)
      {

      }

      @Override
      public List<FailureListener> getFailureListeners()
      {
         return null;
      }

      @Override
      public List<FailureListener> removeFailureListeners()
      {
         return null;
      }

      @Override
      public void setFailureListeners(List<FailureListener> listeners)
      {

      }

      @Override
      public HornetQBuffer createTransportBuffer(int size)
      {
         return new ChannelBufferWrapper(PooledByteBufAllocator.DEFAULT.directBuffer(size), true);
      }

      @Override
      public void fail(HornetQException me)
      {

      }

      @Override
      public void destroy()
      {

      }

      @Override
      public Connection getTransportConnection()
      {
         return null;
      }

      @Override
      public boolean isClient()
      {
         return false;
      }

      @Override
      public boolean isDestroyed()
      {
         return false;
      }

      @Override
      public void disconnect(boolean criticalError)
      {

      }

      @Override
      public boolean checkDataReceived()
      {
         return false;
      }

      @Override
      public void flush()
      {

      }

      @Override
      public void bufferReceived(Object connectionID, HornetQBuffer buffer)
      {

      }
   }


}
