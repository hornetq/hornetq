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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.asyncio.AIOCallback;
import org.hornetq.core.asyncio.AsynchronousFile;
import org.hornetq.core.asyncio.BufferCallback;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.asyncio.impl.TimedBuffer;
import org.hornetq.core.asyncio.impl.TimedBufferObserver;
import org.hornetq.core.journal.IOCallback;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;

/**
 * 
 * A AIOSequentialFile
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class AIOSequentialFile extends AbstractSequentialFile
{
   private static final Logger log = Logger.getLogger(AIOSequentialFile.class);

   private boolean opened = false;

   private final int maxIO;

   private AsynchronousFile aioFile;

   private final SequentialFileFactory factory;

   private long fileSize = 0;

   private final AtomicLong position = new AtomicLong(0);

   private TimedBuffer timedBuffer;

   private final BufferCallback bufferCallback;

   /** Instead of having AIOSequentialFile implementing the Observer, I have done it on an inner class.
    *  This is the class returned to the factory when the file is being activated. */
   private final TimedBufferObserver timedBufferObserver = new LocalBufferObserver();

   /** A context switch on AIO would make it to synchronize the disk before
       switching to the new thread, what would cause
       serious performance problems. Because of that we make all the writes on
       AIO using a single thread. */
   private final Executor executor;

   /** The pool for Thread pollers */
   private final Executor pollerExecutor;

   public AIOSequentialFile(final SequentialFileFactory factory,
                            final int bufferSize,
                            final long bufferTimeoutMilliseconds,
                            final String directory,
                            final String fileName,
                            final int maxIO,
                            final BufferCallback bufferCallback,
                            final Executor executor,
                            final Executor pollerExecutor)
   {
      super(directory, new File(directory + "/" + fileName));
      this.factory = factory;
      this.maxIO = maxIO;
      this.bufferCallback = bufferCallback;
      this.executor = executor;
      this.pollerExecutor = pollerExecutor;
   }

   public boolean isOpen()
   {
      return opened;
   }

   public int getAlignment() throws Exception
   {
      checkOpened();

      return aioFile.getBlockSize();
   }

   public int calculateBlockStart(final int position) throws Exception
   {
      int alignment = getAlignment();

      int pos = (position / alignment + (position % alignment != 0 ? 1 : 0)) * alignment;

      return pos;
   }

   public boolean fits(int size)
   {
      return timedBuffer.checkSize(size);
   }

   public void disableAutoFlush()
   {
      timedBuffer.disableAutoFlush();
   }

   public void enableAutoFlush()
   {
      timedBuffer.enableAutoFlush();
   }

   public synchronized void close() throws Exception
   {
      if (!opened)
      {
         return;
      }
      opened = false;

      timedBuffer = null;

      final CountDownLatch donelatch = new CountDownLatch(1);

      executor.execute(new Runnable()
      {
         public void run()
         {
            donelatch.countDown();
         }
      });

      while (!donelatch.await(60, TimeUnit.SECONDS))
      {
         log.warn("Executor on file " + getFile().getName() + " couldn't complete its tasks in 60 seconds.",
                  new Exception("Warning: Executor on file " + getFile().getName() +
                                " couldn't complete its tasks in 60 seconds."));
      }

      aioFile.close();
      aioFile = null;

      this.notifyAll();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFile#waitForClose()
    */
   public synchronized void waitForClose() throws Exception
   {
      while (isOpen())
      {
         wait();
      }
   }

   public void fill(final int position, final int size, final byte fillCharacter) throws Exception
   {
      checkOpened();

      int fileblockSize = aioFile.getBlockSize();

      int blockSize = fileblockSize;

      if (size % (100 * 1024 * 1024) == 0)
      {
         blockSize = 100 * 1024 * 1024;
      }
      else if (size % (10 * 1024 * 1024) == 0)
      {
         blockSize = 10 * 1024 * 1024;
      }
      else if (size % (1024 * 1024) == 0)
      {
         blockSize = 1024 * 1024;
      }
      else if (size % (10 * 1024) == 0)
      {
         blockSize = 10 * 1024;
      }
      else
      {
         blockSize = fileblockSize;
      }

      int blocks = size / blockSize;

      if (size % blockSize != 0)
      {
         blocks++;
      }

      int filePosition = position;

      if (position % fileblockSize != 0)
      {
         filePosition = (position / fileblockSize + 1) * fileblockSize;
      }

      aioFile.fill(filePosition, blocks, blockSize, fillCharacter);

      this.fileSize = aioFile.size();
   }

   public void open() throws Exception
   {
      open(maxIO);
   }

   public synchronized void open(final int currentMaxIO) throws Exception
   {
      opened = true;
      aioFile = newFile();
      aioFile.open(getFile().getAbsolutePath(), currentMaxIO);
      position.set(0);
      aioFile.setBufferCallback(bufferCallback);
      this.fileSize = aioFile.size();
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      aioFile.setBufferCallback(callback);
   }

   public void position(final long pos) throws Exception
   {
      position.set(pos);
   }

   public long position() throws Exception
   {
      return position.get();
   }

   public int read(final ByteBuffer bytes, final IOCallback callback) throws Exception
   {
      int bytesToRead = bytes.limit();

      long positionToRead = position.getAndAdd(bytesToRead);

      bytes.rewind();

      aioFile.read(positionToRead, bytesToRead, bytes, callback);

      return bytesToRead;
   }

   public int read(final ByteBuffer bytes) throws Exception
   {
      IOCallback waitCompletion = SimpleWaitIOCallback.getInstance();

      int bytesRead = read(bytes, waitCompletion);

      waitCompletion.waitCompletion();

      return bytesRead;
   }

   public void write(final HornetQBuffer bytes, final boolean sync, final IOCallback callback) throws Exception
   {
      if (timedBuffer != null)
      {
         timedBuffer.addBytes(bytes.array(), sync, callback);
      }
      else
      {
         ByteBuffer buffer = factory.newBuffer(bytes.capacity());
         buffer.put(bytes.array());
         doWrite(buffer, callback);
      }
   }

   public void write(final HornetQBuffer bytes, final boolean sync) throws Exception
   {
      if (sync)
      {
         IOCallback completion = SimpleWaitIOCallback.getInstance();

         write(bytes, true, completion);

         completion.waitCompletion();
      }
      else
      {
         write(bytes, false, DummyCallback.getInstance());
      }
   }

   public void write(final ByteBuffer bytes, final boolean sync, final IOCallback callback) throws Exception
   {
      if (timedBuffer != null)
      {
         // sanity check.. it shouldn't happen
         log.warn("Illegal buffered usage. Can't use ByteBuffer write while buffer SequentialFile");
      }

      doWrite(bytes, callback);
   }

   public void write(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      if (sync)
      {
         IOCallback completion = SimpleWaitIOCallback.getInstance();

         write(bytes, true, completion);

         completion.waitCompletion();
      }
      else
      {
         write(bytes, false, DummyCallback.getInstance());
      }
   }

   public void sync() throws Exception
   {
      throw new IllegalArgumentException("This method is not supported on AIO");
   }

   public long size() throws Exception
   {
      if (aioFile == null)
      {
         return getFile().length();
      }
      else
      {
         return aioFile.size();
      }
   }

   @Override
   public String toString()
   {
      return "AIOSequentialFile:" + getFile().getAbsolutePath();
   }

   // Public methods
   // -----------------------------------------------------------------------------------------------------

   public void setTimedBuffer(TimedBuffer buffer)
   {
      if (timedBuffer != null)
      {
         timedBuffer.setObserver(null);
      }

      this.timedBuffer = buffer;

      if (buffer != null)
      {
         buffer.setObserver(this.timedBufferObserver);
      }

   }

   // Protected methods
   // -----------------------------------------------------------------------------------------------------

   /**
    * An extension point for tests
    */
   protected AsynchronousFile newFile()
   {
      return new AsynchronousFileImpl(executor, pollerExecutor);
   }

   // Private methods
   // -----------------------------------------------------------------------------------------------------

   private void doWrite(final ByteBuffer bytes, final IOCallback callback)
   {
      final int bytesToWrite = factory.calculateBlockSize(bytes.limit());

      final long positionToWrite = position.getAndAdd(bytesToWrite);

      aioFile.write(positionToWrite, bytesToWrite, bytes, callback);
   }

   private void checkOpened() throws Exception
   {
      if (aioFile == null || !opened)
      {
         throw new IllegalStateException("File not opened");
      }
   }

   private static class DelegateCallback implements IOCallback
   {
      final List<AIOCallback> delegates;

      DelegateCallback(List<AIOCallback> delegates)
      {
         this.delegates = delegates;
      }

      public void done()
      {
         for (AIOCallback callback : delegates)
         {
            try
            {
               callback.done();
            }
            catch (Throwable e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }

      public void onError(int errorCode, String errorMessage)
      {
         for (AIOCallback callback : delegates)
         {
            try
            {
               callback.onError(errorCode, errorMessage);
            }
            catch (Throwable e)
            {
               log.warn(e.getMessage(), e);
            }
         }
      }

      public void waitCompletion() throws Exception
      {
      }
   }

   class LocalBufferObserver implements TimedBufferObserver
   {

      public void flushBuffer(ByteBuffer buffer, List<AIOCallback> callbacks)
      {
         buffer.flip();

         if (buffer.limit() == 0)
         {
            factory.releaseBuffer(buffer);
         }
         else
         {
            doWrite(buffer, new DelegateCallback(callbacks));
         }
      }

      public ByteBuffer newBuffer(int size, int limit)
      {
         size = factory.calculateBlockSize(size);
         limit = factory.calculateBlockSize(limit);

         ByteBuffer buffer = factory.newBuffer(size);
         buffer.limit(limit);
         return buffer;
      }

      public int getRemainingBytes()
      {
         if (fileSize - position.get() > Integer.MAX_VALUE)
         {
            return Integer.MAX_VALUE;
         }
         else
         {
            return (int)(fileSize - position.get());
         }
      }

      public String toString()
      {
         return "TimedBufferObserver on file (" + getFile().getName() + ")";
      }

   }
}
