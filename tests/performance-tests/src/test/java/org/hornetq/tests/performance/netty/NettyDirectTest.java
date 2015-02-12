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
import io.netty.buffer.UnpooledByteBufAllocator;
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
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.ReusableLatch;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author clebertsuconic
 */
@RunWith(Parameterized.class)
public class NettyDirectTest
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


   public NettyDirectTest(String name, int executorOption)
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
//
//      bootstrapConnector.option(ChannelOption.SO_KEEPALIVE, true);
//      bootstrapConnector.option(ChannelOption.SO_REUSEADDR, true);
      bootstrapConnector.option(ChannelOption.ALLOCATOR, new UnpooledByteBufAllocator(false));


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

      if (!future.isSuccess())
      {
         throw future.cause();
      }
      else
      {
         long time = System.currentTimeMillis();
         long timeStart = System.currentTimeMillis() + 20000;
         Channel channel = future.channel();
         int initCount = 0;
         int max = Integer.MAX_VALUE;
         for (int i = 0; i < max; i++)
         {
            if (initCount == 0 && System.currentTimeMillis() > timeStart)
            {
               System.out.println("heat up gone");
               initCount = i;
               max = initCount + 200000;
               time = System.currentTimeMillis();
            }
            ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(1500);
            buf.writeInt(4);
            buf.writeInt(i);
            latch.countUp();
            channel.writeAndFlush(buf);
            latch.await();
         }

         printCommitsSecond(time, (max - initCount));

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
       * Calls {@link ChannelHandlerContext#fireExceptionCaught(Throwable)} to forward
       * to the next {@link io.netty.channel.ChannelHandler} in the {@link ChannelPipeline}.
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
         ByteBuf buffer = (ByteBuf) msg;

         buffer.readInt();

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

         this.tccl = NettyDirectTest.this.getClass().getClassLoader();
      }

      public Thread newThread(final Runnable command)
      {
         final Thread t = new Thread(group, command, "TestThread-" + threadCount.getAndIncrement() + " (" + group.getName() + ")");
         t.setContextClassLoader(tccl);
         t.setPriority(1);

         return t;
      }
   }

   public class ServerHandler extends ByteToMessageDecoder
   {
      public ServerHandler()
      {
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
      {
         super.channelRead(ctx, msg);
      }

      @Override
      protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
      {
         final int response = in.readInt();

         if (executorOption == 0)
         {
            respond(response);
         }
         else if (executorOption == 1)
         {
            executor.execute(new Runnable()
            {
               public void run()
               {
                  respond(response);
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
                  respond(response);
               }
            });
            ctx.flush();
         }
      }

      private void respond(int response)
      {
         ByteBuf bufAnswer = PooledByteBufAllocator.DEFAULT.buffer(1500);
         bufAnswer.writeInt(4);
         bufAnswer.writeInt(response);
         serverCommunicatingChannel.writeAndFlush(bufAnswer);
      }

   }


}
