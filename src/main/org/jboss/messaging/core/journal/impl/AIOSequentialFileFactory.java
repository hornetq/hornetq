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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.utils.JBMThreadFactory;

/**
 * 
 * A AIOSequentialFileFactory
 * 
 * @author clebert.suconic@jboss.com
 *
 */
public class AIOSequentialFileFactory extends AbstractSequentialFactory
{
   
   /** A single AIO write executor for every AIO File.
    *  This is used only for AIO & instant operations. We only need one executor-thread for the entire journal as we always have only one active file.
    *  And even if we had multiple files at a given moment, this should still be ok, as we control max-io in a semaphore, guaranteeing AIO calls don't block on disk calls */
   private final Executor writeExecutor = Executors.newSingleThreadExecutor(new JBMThreadFactory("JBM-AIO-writer-pool" + System.identityHashCode(this), true));
   

   private final Executor pollerExecutor = Executors.newCachedThreadPool(new JBMThreadFactory("JBM-AIO-poller-pool" + System.identityHashCode(this), true));


   public AIOSequentialFileFactory(final String journalDir)
   {
      super(journalDir);
   }

   public SequentialFile createSequentialFile(final String fileName, final int maxIO)
   {
      return new AIOSequentialFile(journalDir, fileName, maxIO, bufferCallback, writeExecutor, pollerExecutor);
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
      return AsynchronousFileImpl.newBuffer(size);
   }

   public void clearBuffer(final ByteBuffer directByteBuffer)
   {
      AsynchronousFileImpl.clearBuffer(directByteBuffer);
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

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.journal.SequentialFileFactory#releaseBuffer(java.nio.ByteBuffer)
    */
   public void releaseBuffer(ByteBuffer buffer)
   {
      AsynchronousFileImpl.destroyBuffer(buffer);
   }
}
