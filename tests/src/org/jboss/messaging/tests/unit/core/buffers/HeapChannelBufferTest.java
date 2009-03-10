/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @author tags. See the COPYRIGHT.txt in the distribution for a
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

/**
 *
 * @author The Netty Project (netty-dev@lists.jboss.org)
 * @author Trustin Lee (tlee@redhat.com)
 * Stripped down by Clebert Suconic for JBM
 *
 * @version $Rev: 237 $, $Date: 2008-09-04 06:53:44 -0500 (Thu, 04 Sep 2008) $
 */
public class HeapChannelBufferTest extends ChannelBuffersTestBase
{

   @Override
   protected ChannelBuffer newBuffer(final int length)
   {
      ChannelBuffer buffer = ChannelBuffers.buffer(length);
      assertEquals(0, buffer.writerIndex());
      return buffer;
   }

   public void testShouldNotAllowNullInConstructor()
   {
      try
      {
         ChannelBuffers.wrappedBuffer((byte[])null);
         fail("Exception expected");
      }
      catch (NullPointerException e)
      {

      }
   }
}
