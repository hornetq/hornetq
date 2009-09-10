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

package org.hornetq.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.hornetq.core.asyncio.BufferCallback;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.asyncio.impl.TimedBuffer;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.HornetQThreadFactory;

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
   private final Executor writeExecutor = Executors.newSingleThreadExecutor(new HornetQThreadFactory("HornetQ-AIO-writer-pool" + System.identityHashCode(this),
                                                                                                 true));

   private final Executor pollerExecutor = Executors.newCachedThreadPool(new HornetQThreadFactory("HornetQ-AIO-poller-pool" + System.identityHashCode(this),
                                                                                              true));

   private final int bufferSize;

   private final long bufferTimeout;

   private final TimedBuffer timedBuffer;

   public AIOSequentialFileFactory(final String journalDir)
   {
      this(journalDir,
           ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_SIZE,
           ConfigurationImpl.DEFAULT_JOURNAL_AIO_BUFFER_TIMEOUT,
           ConfigurationImpl.DEFAULT_JOURNAL_AIO_FLUSH_SYNC,
           false);
   }

   public AIOSequentialFileFactory(final String journalDir,
                                   int bufferSize,
                                   long bufferTimeout,
                                   boolean flushOnSync,
                                   boolean logRates)
   {
      super(journalDir);
      this.bufferSize = bufferSize;
      this.bufferTimeout = bufferTimeout;
      this.timedBuffer = new TimedBuffer(bufferSize, bufferTimeout, flushOnSync, logRates);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFileFactory#activate(org.hornetq.core.journal.SequentialFile)
    */
   public void activate(SequentialFile file)
   {
      final AIOSequentialFile sequentialFile = (AIOSequentialFile)file;
      timedBuffer.disableAutoFlush();
      try
      {
         sequentialFile.setTimedBuffer(timedBuffer);
      }
      finally
      {
         timedBuffer.enableAutoFlush();
      }
   }

   public void testFlush()
   {
      timedBuffer.flush();
   }

   public void deactivate(SequentialFile file)
   {
      timedBuffer.flush();
      timedBuffer.setObserver(null);
   }

   public SequentialFile createSequentialFile(final String fileName, final int maxIO)
   {
      return new AIOSequentialFile(this,
                                   bufferSize,
                                   bufferTimeout,
                                   journalDir,
                                   fileName,
                                   maxIO,
                                   buffersControl.callback,
                                   writeExecutor,
                                   pollerExecutor);
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
    * @see org.hornetq.core.journal.SequentialFileFactory#releaseBuffer(java.nio.ByteBuffer)
    */
   public void releaseBuffer(ByteBuffer buffer)
   {
      AsynchronousFileImpl.destroyBuffer(buffer);
   }

   public void start()
   {
      timedBuffer.start();
   }

   public void stop()
   {
      buffersControl.stop();
      timedBuffer.stop();
   }

   protected void finalize()
   {
      this.stop();
   }

   /** Class that will control buffer-reuse */
   private class ReuseBuffersController
   {
      private volatile long bufferReuseLastTime = System.currentTimeMillis();

      /** This queue is fed by {@link JournalImpl.ReuseBuffersController.LocalBufferCallback}} which is called directly by NIO or NIO.
       * On the case of the AIO this is almost called by the native layer as soon as the buffer is not being used any more
       * and ready to be reused or GCed */
      private final ConcurrentLinkedQueue<ByteBuffer> reuseBuffersQueue = new ConcurrentLinkedQueue<ByteBuffer>();

      private boolean stopped = false;

      final BufferCallback callback = new LocalBufferCallback();

      public ByteBuffer newBuffer(final int size)
      {
         // if a new buffer wasn't requested in 10 seconds, we clear the queue
         // This is being done this way as we don't need another Timeout Thread
         // just to cleanup this
         if (bufferSize > 0 && System.currentTimeMillis() - bufferReuseLastTime > 10000)
         {
            if (trace)
               trace("Clearing reuse buffers queue with " + reuseBuffersQueue.size() + " elements");

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

      public synchronized void stop()
      {
         stopped = true;
         clearPoll();
      }

      public synchronized void clearPoll()
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
            synchronized (ReuseBuffersController.this)
            {

               if (stopped)
               {
                  releaseBuffer(buffer);
               }
               else
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

}
