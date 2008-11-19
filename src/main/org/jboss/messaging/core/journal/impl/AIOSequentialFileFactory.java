/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.journal.impl;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.SequentialFile;

/**
 * 
 * A AIOSequentialFileFactory
 * 
 * @author clebert.suconic@jboss.com
 *
 */
public class AIOSequentialFileFactory extends AbstractSequentialFactory
{
   public AIOSequentialFileFactory(final String journalDir)
   {
      super(journalDir);
   }

   public SequentialFile createSequentialFile(final String fileName, final int maxIO)
   {
      return new AIOSequentialFile(journalDir, fileName, maxIO);
   }

   public boolean isSupportsCallbacks()
   {
      return true;
   }

   public static boolean isSupported()
   {
      return AsynchronousFileImpl.isLoaded();
   }

   public ByteBuffer newBuffer(int size)
   {
      if (size % 512 != 0)
      {
         size = (size / 512 + 1) * 512;
      }
      return ByteBuffer.allocateDirect(size);
   }

   public void clearBuffer(final ByteBuffer directByteBuffer)
   {
      AsynchronousFileImpl.resetBuffer(directByteBuffer, directByteBuffer.limit());
   }

   public int getAlignment()
   {
      return 512;
   }

   // For tests only
   public ByteBuffer wrapBuffer(final byte[] bytes)
   {
      ByteBuffer newbuffer = newBuffer(bytes.length);
      newbuffer.put(bytes);
      return newbuffer;
   }

   public int calculateBlockSize(final int position)
   {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }
}
