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
import java.util.concurrent.atomic.AtomicLong;

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

   private File file;
   
   private long fileSize = 0;

   private final String directory;

   private FileChannel channel;

   private RandomAccessFile rfile;
   
   private final AtomicLong position = new AtomicLong(0);

   public NIOSequentialFile(final String directory, final String fileName)
   {
      this.directory = directory;
      file = new File(directory + "/" + fileName);
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

   public String getFileName()
   {
      return file.getName();
   }

   public synchronized boolean isOpen()
   {
      return channel != null;
   }

   public synchronized void open() throws Exception
   {
      rfile = new RandomAccessFile(file, "rw");

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

   public void close() throws Exception
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
   }

   public void delete() throws Exception
   {
      if (isOpen())
      {
         close();
      }

      file.delete();
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
      channel.force(false);
   }

   public long size() throws Exception
   {
      return channel.size();
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

   public void renameTo(final String newFileName) throws Exception
   {
      close();
      File newFile = new File(directory + "/" + newFileName);
      file.renameTo(newFile);
      file = newFile;
   }

   @Override
   public String toString()
   {
      return "NIOSequentialFile " + file;
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.journal.SequentialFile#setBuffering(boolean)
    */
   public void setBuffering(boolean buffering)
   {
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.journal.SequentialFile#lockBuffer()
    */
   public void lockBuffer()
   {
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.journal.SequentialFile#unlockBuffer()
    */
   public void unlockBuffer()
   {
   }

}
