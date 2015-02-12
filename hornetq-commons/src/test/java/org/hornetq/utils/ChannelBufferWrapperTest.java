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

package org.hornetq.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.hornetq.core.buffers.impl.ChannelBufferWrapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author clebertsuconic
 */

public class ChannelBufferWrapperTest
{
   @Test
   public void testUnwrap()
   {
      ByteBuf buff = UnpooledByteBufAllocator.DEFAULT.heapBuffer(100);

      ByteBuf wrapped = buff;

      for (int i = 0; i < 10; i++)
      {
         wrapped = Unpooled.unreleasableBuffer(wrapped);
      }


      // If this starts to loop forever it means that Netty has changed
      // the semantic of Unwrap call and it's returning itself
      Assert.assertEquals(buff, ChannelBufferWrapper.unwrap(wrapped));

      // does it work with itself as well?
      Assert.assertEquals(buff, ChannelBufferWrapper.unwrap(buff));
   }
}
