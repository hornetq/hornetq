/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.unit.core.buffers;

import org.jboss.messaging.core.buffers.ChannelBuffer;
import org.jboss.messaging.core.buffers.ChannelBuffers;
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
    * @see org.jboss.messaging.tests.unit.core.buffers.AbstractChannelBufferTest#newBuffer(int)
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
