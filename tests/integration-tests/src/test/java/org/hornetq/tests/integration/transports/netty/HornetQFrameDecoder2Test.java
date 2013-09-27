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

package org.hornetq.tests.integration.transports.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;

import org.hornetq.core.remoting.impl.netty.HornetQFrameDecoder2;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A HornetQFrameDecoder2Test
 *
 * @author <a href="tlee@redhat.com">Trustin Lee</a>
 * @version $Revision$, $Date$
 */
public class HornetQFrameDecoder2Test extends UnitTestCase
{
   private static final int MSG_CNT = 10000;

   private static final int MSG_LEN = 1000;

   private static final int FRAGMENT_MAX_LEN = 1500;

   private static final Random rand = new Random();

   @Test
   public void testOrdinaryFragmentation() throws Exception
   {
      final EmbeddedChannel decoder = new EmbeddedChannel(new HornetQFrameDecoder2());

      ByteBuf src = Unpooled.buffer(HornetQFrameDecoder2Test.MSG_CNT * (HornetQFrameDecoder2Test.MSG_LEN + 4));
      while (src.writerIndex() < src.capacity())
      {
         src.writeInt(HornetQFrameDecoder2Test.MSG_LEN);
         byte[] data = new byte[HornetQFrameDecoder2Test.MSG_LEN];
         HornetQFrameDecoder2Test.rand.nextBytes(data);
         src.writeBytes(data);
      }

      List<ByteBuf> packets = new ArrayList<ByteBuf>();
      for (int i = 0; i < src.capacity();)
      {
         int length = Math.min(HornetQFrameDecoder2Test.rand.nextInt(HornetQFrameDecoder2Test.FRAGMENT_MAX_LEN),
                               src.capacity() - i);
         packets.add(src.copy(i, length));
         i += length;
      }

      int cnt = 0;
      for (int i = 0; i < packets.size(); i++)
      {
         ByteBuf p = packets.get(i);
         decoder.writeInbound(p.duplicate());
         for (;;)
         {
            ByteBuf frame = (ByteBuf) decoder.readInbound();
            if (frame == null)
            {
               break;
            }
            Assert.assertEquals(4, frame.readerIndex());
            Assert.assertEquals(HornetQFrameDecoder2Test.MSG_LEN, frame.readableBytes());
            Assert.assertEquals(src.slice(cnt * (HornetQFrameDecoder2Test.MSG_LEN + 4) + 4,
                                          HornetQFrameDecoder2Test.MSG_LEN), frame);
            cnt++;
            frame.release();
         }
      }
      Assert.assertEquals(HornetQFrameDecoder2Test.MSG_CNT, cnt);
   }

   @Test
   public void testExtremeFragmentation() throws Exception
   {
      final EmbeddedChannel decoder = new EmbeddedChannel(new HornetQFrameDecoder2());

      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 0 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 0 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 0 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 4 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 5 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 6 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 7 }));
      Assert.assertNull(decoder.readInbound());
      decoder.writeInbound(Unpooled.wrappedBuffer(new byte[] { 8 }));

      ByteBuf frame = (ByteBuf) decoder.readInbound();
      Assert.assertEquals(4, frame.readerIndex());
      Assert.assertEquals(4, frame.readableBytes());
      Assert.assertEquals(5, frame.getByte(4));
      Assert.assertEquals(6, frame.getByte(5));
      Assert.assertEquals(7, frame.getByte(6));
      Assert.assertEquals(8, frame.getByte(7));
      frame.release();
   }
}
