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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.journal.BufferCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.logging.Logger;
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
   
   

   private static final Logger log = Logger.getLogger(AIOSequentialFileFactory.class);

   private static final boolean trace = log.isTraceEnabled();
   
   
   private final ReuseBuffersController buffersControl = new ReuseBuffersController();

   // This method exists just to make debug easier.
   // I could replace log.trace by log.info temporarily while I was debugging
   // Journal
   private static final void trace(final String message)
   {
      log.trace(message);
   }

   /** A single AIO write executor for every AIO File.
    *  This is used only for AIO & instant operations. We only need one executor-thread for the entire journal as we always have only one active file.
    *  And even if we had multiple files at a given moment, this should still be ok, as we control max-io in a semaphore, guaranteeing AIO calls don't block on disk calls */
   private final Executor writeExecutor = Executors.newSingleThreadExecutor(new JBMThreadFactory("JBM-AIO-writer-pool" + System.identityHashCode(this), true));
   

   private final Executor pollerExecutor = Executors.newCachedThreadPool(new JBMThreadFactory("JBM-AIO-poller-pool" + System.identityHashCode(this), true));

   
   final int bufferSize;
   
   final int bufferTimeout;
   
   public AIOSequentialFileFactory(final String journalDir)
   {
      this(journalDir, ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_SIZE, ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_TIMEOUT);
   }

   public AIOSequentialFileFactory(final String journalDir, int bufferSize, int bufferTimeout)
   {
      super(journalDir);
      this.bufferSize = bufferSize;
      this.bufferTimeout = bufferTimeout;
   }

   public SequentialFile createSequentialFile(final String fileName, final int maxIO)
   {
      return new AIOSequentialFile(this, bufferSize, bufferTimeout, journalDir, fileName, maxIO, buffersControl.callback, writeExecutor, pollerExecutor);
   }

   public boolean isSupportsCallbacks()
   {
      return true;
   }

   public static boolean isSupported()
   {
      return AsynchronousFileImpl.isLoaded();
   }
   
   public void controlBuffersLifeCycle(boolean value)
   {
      if (value)
      {
         buffersControl.enable();
      }
      else
      {
         buffersControl.disable();
      }
   }

   public ByteBuffer newBuffer(int size)
   {
      if (size % 512 != 0)
      {
         size = (size / 512 + 1) * 512;
      }
      
      return buffersControl.newBuffer(size);
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
   
   public void stop()
   {
      buffersControl.clearPoll();
   }
   
   
   /** Class that will control buffer-reuse */
   private class ReuseBuffersController
   {
      private volatile long bufferReuseLastTime = System.currentTimeMillis();

      /** This queue is fed by {@link JournalImpl.ReuseBuffersController.LocalBufferCallback}} which is called directly by NIO or NIO.
       * On the case of the AIO this is almost called by the native layer as soon as the buffer is not being used any more
       * and ready to be reused or GCed */
      private final ConcurrentLinkedQueue<ByteBuffer> reuseBuffersQueue = new ConcurrentLinkedQueue<ByteBuffer>();
      
      /** During reload we may disable/enable buffer reuse */
      private boolean enabled = true;

      final BufferCallback callback = new LocalBufferCallback();
      
      public void enable()
      {
         this.enabled = true;
      }
      
      public void disable()
      {
         this.enabled = false;
      }

      public ByteBuffer newBuffer(final int size)
      {
         // if a new buffer wasn't requested in 10 seconds, we clear the queue
         // This is being done this way as we don't need another Timeout Thread
         // just to cleanup this
         if (bufferSize > 0 && System.currentTimeMillis() - bufferReuseLastTime > 10000)
         {
            if (trace) trace("Clearing reuse buffers queue with " + reuseBuffersQueue.size() + " elements");

            bufferReuseLastTime = System.currentTimeMillis();

            clearPoll();
         }

         // if a buffer is bigger than the configured-bufferSize, we just create a new
         // buffer.
         if (size > bufferSize)
         {
            return AsynchronousFileImpl.newBuffer(size);
         }
         else
         {
            // We need to allocate buffers following the rules of the storage
            // being used (AIO/NIO)
            int alignedSize = calculateBlockSize(size);

            // Try getting a buffer from the queue...
            ByteBuffer buffer = reuseBuffersQueue.poll();

            if (buffer == null)
            {
               // if empty create a new one.
               buffer = AsynchronousFileImpl.newBuffer(bufferSize);

               buffer.limit(alignedSize);
            }
            else
            {
               clearBuffer(buffer);

               // set the limit of the buffer to the bufferSize being required
               buffer.limit(alignedSize);
            }
            
            buffer.rewind();

            return buffer;
         }
      }

      public void clearPoll()
      {
         ByteBuffer reusedBuffer;
         
         while ((reusedBuffer = reuseBuffersQueue.poll()) != null)
         {
            releaseBuffer(reusedBuffer);
         }
      }

      private class LocalBufferCallback implements BufferCallback
      {
         public void bufferDone(final ByteBuffer buffer)
         {
            if (enabled)
            {
               bufferReuseLastTime = System.currentTimeMillis();
   
               // If a buffer has any other than the configured bufferSize, the buffer
               // will be just sent to GC
               if (buffer.capacity() == bufferSize)
               {
                  reuseBuffersQueue.offer(buffer);
               }
               else
               {
                  releaseBuffer(buffer);
               }
            }
         }
      }
   }

   
}
