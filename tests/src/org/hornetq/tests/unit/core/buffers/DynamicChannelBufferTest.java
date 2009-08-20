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

package org.hornetq.tests.unit.core.buffers;

import org.hornetq.core.buffers.ChannelBuffer;
import org.hornetq.core.buffers.ChannelBuffers;
import org.jboss.netty.buffer.DynamicChannelBuffer;

/**
 * A DynamicChannelBufferTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Mar 10, 2009 9:26:36 AM
 *
 *
 */
public class DynamicChannelBufferTest extends ChannelBuffersTestBase
{

   /* (non-Javadoc)
    * @see org.hornetq.tests.unit.core.buffers.AbstractChannelBufferTest#newBuffer(int)
    */
   @Override
   protected ChannelBuffer newBuffer(final int length)
   {
      ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(length);

      // A dynamic buffer does lazy initialization.
      assertEquals(0, buffer.capacity());

      // Initialize.
      buffer.writeZero(1);
      buffer.clear();

      // And validate the initial capacity
      assertEquals(0, buffer.writerIndex());
      assertEquals(length, buffer.capacity());

      return buffer;
   }

   public void testShouldNotAllowNullInConstructor()
   {
      try
      {
         new DynamicChannelBuffer(null, 0);
         fail("Exception expected");
      }
      catch (NullPointerException e)
      {
      }
   }
   
   public void testExpanding()
   {
      ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(10);
      
      for (byte b = 0; b < (byte)20; b++)
      {
         buffer.writeByte(b);
      }
      
      for (byte b = 0; b < (byte)20; b++)
      {
         assertEquals(b, buffer.readByte());
      }
      
      
      assertEquals(20, buffer.writerIndex());
      assertEquals(20, buffer.readerIndex());
   }
   
   
   public void testReuse()
   {
      byte[] bytes = new byte[10];
      for (byte b = 0; b < (byte)10; b++)
      {
         bytes[b] = b;
      }
      

      ChannelBuffer buffer = ChannelBuffers.dynamicBuffer(bytes);

      buffer.clear();
      
      for (byte b = 0; b < (byte)10; b++)
      {
         buffer.writeByte((byte)(b * 2));
      }
      
      
      // The DynamicBuffer is sharing the byte[].
      for (byte b = 0; b < (byte)10; b++)
      {
         assertEquals((byte)(b * 2), bytes[b]);
      }
      

      buffer.clear();
      
      for (byte b = 0; b < (byte)20; b++)
      {
         buffer.writeByte(b);
      }
      
      for (byte b = 0; b < (byte)20; b++)
      {
         assertEquals(b, buffer.readByte());
      }
      
      
      assertEquals(20, buffer.writerIndex());
      assertEquals(20, buffer.readerIndex());
      
      
      
   }

}
