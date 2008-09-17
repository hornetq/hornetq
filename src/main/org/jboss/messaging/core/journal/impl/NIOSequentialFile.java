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

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.jboss.messaging.core.journal.BufferCallback;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A NIOSequentialFile
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class NIOSequentialFile implements SequentialFile
{
   private static final Logger log = Logger.getLogger(NIOSequentialFile.class);

   private final String journalDir;

   private final String fileName;

   private File file;

   private FileChannel channel;

   private RandomAccessFile rfile;

   BufferCallback bufferCallback;

   public NIOSequentialFile(final String journalDir, final String fileName)
   {
      this.journalDir = journalDir;

      this.fileName = fileName;
   }

   public int getAlignment()
   {
      return 1;
   }

   public int calculateBlockStart(final int position) throws Exception
   {
      return position;
   }

   public String getFileName()
   {
      return fileName;
   }

   public synchronized void open() throws Exception
   {
      file = new File(journalDir + "/" + fileName);

      rfile = new RandomAccessFile(file, "rw");

      channel = rfile.getChannel();
   }

   public void open(final int currentMaxIO) throws Exception
   {
      open();
   }

   public void setBufferCallback(final BufferCallback callback)
   {
      bufferCallback = callback;
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
   }

   public void close() throws Exception
   {
      channel.close();

      rfile.close();

      channel = null;

      rfile = null;

      file = null;
   }

   public void delete() throws Exception
   {
      file.delete();

      close();
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

   public int write(final ByteBuffer bytes, final boolean sync) throws Exception
   {
      int bytesRead = channel.write(bytes);

      if (sync)
      {
         sync();
      }

      if (bufferCallback != null)
      {
         bufferCallback.bufferDone(bytes);
      }

      return bytesRead;
   }

   public int write(final ByteBuffer bytes, final IOCallback callback) throws Exception
   {
      try
      {
         int bytesRead = channel.write(bytes);

         if (callback != null)
         {
            callback.done();
         }

         if (bufferCallback != null)
         {
            bufferCallback.bufferDone(bytes);
         }

         return bytesRead;
      }
      catch (Exception e)
      {
         callback.onError(-1, e.getMessage());
         throw e;
      }
   }

   public void sync() throws Exception
   {
      channel.force(false);
   }

   public long size() throws Exception
   {
      return channel.size();
   }

   public void position(final int pos) throws Exception
   {
      channel.position(pos);
   }

   public int position() throws Exception
   {
      return (int)channel.position();
   }

}
