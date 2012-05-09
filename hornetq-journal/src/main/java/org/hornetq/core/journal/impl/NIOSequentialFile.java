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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.IOErrorException;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.journal.HornetQJournalBundle;
import org.hornetq.journal.HornetQJournalLogger;

/**
 *
 * A NIOSequentialFile
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class NIOSequentialFile extends AbstractSequentialFile
{
   private FileChannel channel;

   private RandomAccessFile rfile;

   /** The write semaphore here is only used when writing asynchronously */
   private Semaphore maxIOSemaphore;

   private final int defaultMaxIO;

   private int maxIO;

   public NIOSequentialFile(final SequentialFileFactory factory,
                            final String directory,
                            final String fileName,
                            final int maxIO,
                            final Executor writerExecutor)
   {
      super(directory, new File(directory + "/" + fileName), factory, writerExecutor);
      defaultMaxIO = maxIO;
   }

   public NIOSequentialFile(final SequentialFileFactory factory,
                            final File file,
                            final int maxIO,
                            final Executor writerExecutor)
   {
      super(file.getParent(), new File(file.getPath()), factory, writerExecutor);
      defaultMaxIO = maxIO;
   }

   public int getAlignment()
   {
      return 1;
   }

   public int calculateBlockStart(final int position) throws Exception
   {
      return position;
   }

   public synchronized boolean isOpen()
   {
      return channel != null;
   }

   /** this.maxIO represents the default maxIO.
    *  Some operations while initializing files on the journal may require a different maxIO */
   public synchronized void open() throws Exception
   {
      open(defaultMaxIO, true);
   }

   public void open(final int maxIO, final boolean useExecutor) throws Exception
   {
      try
      {
         rfile = new RandomAccessFile(getFile(), "rw");
   
         channel = rfile.getChannel();
   
         fileSize = channel.size();
      }
      catch (IOException e)
      {
         factory.onIOError(HornetQExceptionType.IO_ERROR, e.getMessage(), this);
         throw e;
      }

      if (writerExecutor != null && useExecutor)
      {
         maxIOSemaphore = new Semaphore(maxIO);
         this.maxIO = maxIO;
      }
   }

   public void fill(final int position, final int size, final byte fillCharacter) throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocate(size);

      for (int i = 0; i < size; i++)
      {
         bb.put(fillCharacter);
      }

      bb.flip();

      channel.position(position);

      channel.write(bb);

      channel.force(false);

      channel.position(0);

      fileSize = channel.size();
   }

   public synchronized void waitForClose() throws Exception
   {
      while (isOpen())
      {
         wait();
      }
   }

   @Override
   public synchronized void close() throws Exception
   {
      super.close();

      if (maxIOSemaphore != null)
      {
         while (!maxIOSemaphore.tryAcquire(maxIO, 60, TimeUnit.SECONDS))
         {
            HornetQJournalLogger.LOGGER.errorClosingFile(getFileName());
         }
      }

      maxIOSemaphore = null;

      if (channel != null)
      {
         channel.close();
      }

      if (rfile != null)
      {
         rfile.close();
      }

      channel = null;

      rfile = null;

      notifyAll();
   }

   public int read(final ByteBuffer bytes) throws Exception
   {
      return read(bytes, null);
   }

   public synchronized int read(final ByteBuffer bytes, final IOAsyncTask callback) throws Exception
   {
      try
      {
         if (channel == null)
         {
            throw new Exception("File " + this.getFileName() + " has a null channel");
         }
         int bytesRead = channel.read(bytes);

         if (callback != null)
         {
            callback.done();
         }

         bytes.flip();

         return bytesRead;
      }
      catch (IOException e)
      {
         if (callback != null)
         {
            callback.onError(HornetQExceptionType.IO_ERROR.getCode(), e.getLocalizedMessage());
         }
         
         factory.onIOError(HornetQExceptionType.IO_ERROR, e.getMessage(), this);

         throw e;
      }
      catch (Exception e)
      {
         throw e;
      }

   }

   public void sync() throws IOException
   {
      if (channel != null)
      {
         channel.force(false);
      }
   }

   public long size() throws Exception
   {
      if (channel == null)
      {
         return getFile().length();
      }
      else
      {
         return channel.size();
      }
   }

   @Override
   public void position(final long pos) throws Exception
   {
      super.position(pos);
      channel.position(pos);
   }

   @Override
   public String toString()
   {
      return "NIOSequentialFile " + getFile();
   }

   public SequentialFile cloneFile()
   {
      return new NIOSequentialFile(factory, getFile(), maxIO, writerExecutor);
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync, final IOAsyncTask callback)
   {
      if (callback == null)
      {
         throw new NullPointerException("callback parameter need to be set");
      }

      try
      {
         internalWrite(bytes, sync, callback);
      }
      catch (Exception e)
      {
         callback.onError(HornetQExceptionType.GENERIC_EXCEPTION.getCode(), e.getMessage());
      }
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      internalWrite(bytes, sync, null);
   }

   public void writeInternal(final ByteBuffer bytes) throws Exception
   {
      internalWrite(bytes, true, null);
   }

   @Override
   protected ByteBuffer newBuffer(int size, final int limit)
   {
      // For NIO, we don't need to allocate a buffer the entire size of the timed buffer, unlike AIO

      size = limit;

      return super.newBuffer(size, limit);
   }

   private void internalWrite(final ByteBuffer bytes, final boolean sync, final IOAsyncTask callback) throws Exception
   {
      if (!isOpen())
      {
         if (callback != null)
         {
            callback.onError(HornetQExceptionType.IO_ERROR.getCode(), "File not opened");
         }
         else
         {
            throw HornetQJournalBundle.BUNDLE.fileNotOpened();
         }
         return;
      }

      position.addAndGet(bytes.limit());

      if (maxIOSemaphore == null || callback == null)
      {
         // if maxIOSemaphore == null, that means we are not using executors and the writes are synchronous
         try
         {
            doInternalWrite(bytes, sync, callback);
         }
         catch (IOException e)
         {
            factory.onIOError(HornetQExceptionType.IO_ERROR, e.getMessage(), this);
         }
      }
      else
      {
         // This is a flow control on writing, just like maxAIO on libaio
         maxIOSemaphore.acquire();

         writerExecutor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  try
                  {
                     doInternalWrite(bytes, sync, callback);
                  }
                  catch (IOException e)
                  {
                     HornetQJournalLogger.LOGGER.errorSubmittingWrite(e);
                     factory.onIOError(HornetQExceptionType.IO_ERROR, e.getMessage(), NIOSequentialFile.this);
                     callback.onError(HornetQExceptionType.IO_ERROR.getCode(), e.getMessage());
                  }
                  catch (Throwable e)
                  {
                     HornetQJournalLogger.LOGGER.errorSubmittingWrite(e);
                     callback.onError(HornetQExceptionType.IO_ERROR.getCode(), e.getMessage());
                  }
               }
               finally
               {
                  maxIOSemaphore.release();
               }
            }
         });
      }
   }

   /**
    * @param bytes
    * @param sync
    * @param callback
    * @throws IOException
    * @throws Exception
    */
   private void doInternalWrite(final ByteBuffer bytes, final boolean sync, final IOAsyncTask callback) throws IOException
   {
      channel.write(bytes);

      if (sync)
      {
         sync();
      }

      if (callback != null)
      {
         callback.done();
      }
   }
}
