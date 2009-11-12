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

import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;

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

   private FileChannel channel;

   private RandomAccessFile rfile;

   public NIOSequentialFile(final SequentialFileFactory factory, final String directory, final String fileName)
   {
      super(directory, new File(directory + "/" + fileName), factory);
   }

   public NIOSequentialFile(final SequentialFileFactory factory, final File file)
   {
      super(file.getParent(), new File(file.getPath()), factory);
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

   public int read(final ByteBuffer bytes, final IOCompletion callback) throws Exception
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
      super.position(pos);
      channel.position(pos);
   }

   @Override
   public String toString()
   {
      return "NIOSequentialFile " + getFile();
   }

   public SequentialFile copy()
   {
      return new NIOSequentialFile(factory, getFile());
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync, final IOCompletion callback)
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
         callback.onError(-1, e.getMessage());
      }
   }

   public void writeDirect(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      internalWrite(bytes, sync, null);
   }

   /**
    * @param bytes
    * @param sync
    * @param callback
    * @throws IOException
    * @throws Exception
    */
   private void internalWrite(final ByteBuffer bytes, final boolean sync, final IOCompletion callback) throws Exception
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
}
