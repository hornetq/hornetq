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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.journal.IOCallback;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;

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
   private static final Logger log = Logger.getLogger(NIOSequentialFile.class);

   private long fileSize = 0;

   private FileChannel channel;

   private RandomAccessFile rfile;

   private final AtomicLong position = new AtomicLong(0);

   public NIOSequentialFile(final String directory, final String fileName)
   {
      super(directory, new File(directory + "/" + fileName));
   }

   public int getAlignment()
   {
      return 1;
   }

   public void flush()
   {
   }

   public int calculateBlockStart(final int position) throws Exception
   {
      return position;
   }

   public boolean fits(final int size)
   {
      return this.position.get() + size <= fileSize;
   }

   public synchronized boolean isOpen()
   {
      return channel != null;
   }

   public synchronized void open() throws Exception
   {
      rfile = new RandomAccessFile(getFile(), "rw");

      channel = rfile.getChannel();

      fileSize = channel.size();
   }

   public void open(final int currentMaxIO) throws Exception
   {
      open();
   }

   public void fill(final int position, final int size, final byte fillCharacter) throws Exception
   {
      ByteBuffer bb = ByteBuffer.allocateDirect(size);

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

   public synchronized void close() throws Exception
   {
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

   public int read(final ByteBuffer bytes, final IOCallback callback) throws Exception
   {
      try
      {
         int bytesRead = channel.read(bytes);
         if (callback != null)
         {
            callback.done();
         }
         bytes.flip();
         return bytesRead;
      }
      catch (Exception e)
      {
         if (callback != null)
         {
            callback.onError(-1, e.getLocalizedMessage());
         }

         throw e;
      }

   }

   public void write(final HornetQBuffer bytes, final boolean sync) throws Exception
   {
      write(ByteBuffer.wrap(bytes.array()), sync);
   }

   public void write(final HornetQBuffer bytes, final boolean sync, final IOCallback callback) throws Exception
   {
      write(ByteBuffer.wrap(bytes.array()), sync, callback);
   }

   public void write(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      position.addAndGet(bytes.limit());

      channel.write(bytes);

      if (sync)
      {
         sync();
      }
   }

   public void write(final ByteBuffer bytes, final boolean sync, final IOCallback callback) throws Exception
   {
      try
      {
         position.addAndGet(bytes.limit());

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
      catch (Exception e)
      {
         callback.onError(-1, e.getMessage());
         throw e;
      }
   }

   public void sync() throws Exception
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

   public void position(final long pos) throws Exception
   {
      channel.position(pos);
      position.set(pos);
   }

   public long position() throws Exception
   {
      return position.get();
   }

   @Override
   public String toString()
   {
      return "NIOSequentialFile " + getFile();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFile#setBuffering(boolean)
    */
   public void setBuffering(boolean buffering)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFile#lockBuffer()
    */
   public void disableAutoFlush()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.SequentialFile#unlockBuffer()
    */
   public void enableAutoFlush()
   {
   }

}
