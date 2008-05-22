/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.tests.unit.core.journal.impl.fakes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * A FakeSequentialFileFactory
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FakeSequentialFileFactory implements SequentialFileFactory
{
   private static final Logger log = Logger.getLogger(FakeSequentialFileFactory.class);
      
   private Map<String, FakeSequentialFile> fileMap = new ConcurrentHashMap<String, FakeSequentialFile>();
   
   public SequentialFile createSequentialFile(final String fileName, final boolean sync, final int maxAIO, final int timeout) throws Exception
   {
      FakeSequentialFile sf = fileMap.get(fileName);
      
      if (sf == null)
      {                 
         sf = new FakeSequentialFile(fileName, sync);
         
         fileMap.put(fileName, sf);
      }
      else
      {     
         sf.data.position(0);
         
         //log.info("positioning data to 0");
      }
                  
      return sf;
   }
   
   public List<String> listFiles(final String extension)
   {
      List<String> files = new ArrayList<String>();
      
      for (String s: fileMap.keySet())
      {
         if (s.endsWith("." + extension))
         {
            files.add(s);
         }
      }
      
      return files;
   }
   
   public Map<String, FakeSequentialFile> getFileMap()
   {
      return fileMap;
   }
   
   public void clear()
   {
      fileMap.clear();
   }
   
   public boolean supportsCallbacks()
   {
      return false;
   }
   
   public ByteBuffer newBuffer(int size)
   {
      return ByteBuffer.allocate(size);
   }

   public ByteBuffer wrapBuffer(byte[] bytes)
   {
      return ByteBuffer.wrap(bytes);
   }

   public class FakeSequentialFile implements SequentialFile
   {
      private volatile boolean open;
      
      private final String fileName;
      
      private final boolean sync;
      
      private volatile ByteBuffer data;
      
      public ByteBuffer getData()
      {
         return data;
      }
      
      public boolean isSync()
      {
         return sync;
      }
      
      public boolean isOpen()
      {
         //log.info("is open" + System.identityHashCode(this) +" open is now " + open);
         return open;
      }
      
      public FakeSequentialFile(final String fileName, final boolean sync)
      {
         this.fileName = fileName;
         
         this.sync = sync;    
      }

      public void close() throws Exception
      {
         open = false;
         
         if (data != null)
         {
            data.position(0);
         }
      }

      public void delete() throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         close();
         
         fileMap.remove(fileName);
      }

      public String getFileName()
      {
         return fileName;
      }
      
      public void open() throws Exception
      {
         open = true;
      }

      public void fill(int pos, int size, byte fillCharacter) throws Exception
      {     
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         checkAndResize(pos + size);
         
         //log.info("size is " + size + " pos is " + pos);
         
         for (int i = pos; i < size + pos; i++)
         {
            data.array()[i] = fillCharacter;
            
            //log.info("Filling " + pos + " with char " + fillCharacter);
         }                 
      }
      
      public int read(ByteBuffer bytes) throws Exception
      {
         return read(bytes, null);
      }
      
      public int read(ByteBuffer bytes, IOCallback callback) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         //log.info("read called " + bytes.array().length);
         
         byte[] bytesRead = new byte[bytes.array().length];
         
         //log.info("reading, data pos is " + data.position() + " data size is " + data.array().length);
         
         data.get(bytesRead);
         
         bytes.put(bytesRead);
         
         bytes.rewind();
         
         if (callback != null) callback.done();
         
         return bytesRead.length;
      }

      public void position(int pos) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         //log.info("reset called");
         
         data.position(pos);
      }

      public int write(ByteBuffer bytes, boolean sync, IOCallback callback) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         int position = data == null ? 0 : data.position();
         
         checkAndResize(bytes.capacity() + position);
         
         //log.info("write called, position is " + data.position() + " bytes is " + bytes.array().length);
         
         data.put(bytes);
         
         if (callback!=null) callback.done();
         
         return bytes.array().length;
         
      }
      
      public int write(ByteBuffer bytes, boolean sync) throws Exception
      {
         return write(bytes, sync, null);
      }
      
      private void checkAndResize(int size)
      {
         int oldpos = data == null ? 0 : data.position();
         
         if (data == null || data.array().length < size)
         {
            byte[] newBytes = new byte[size];
            
            if (data != null)
            {
               System.arraycopy(data.array(), 0, newBytes, 0, data.array().length);
            }
            
            data = ByteBuffer.wrap(newBytes);
            
            data.position(oldpos);
         }
      }

      public int getAlignment() throws Exception
      {
         return 1;
      }

      public int calculateBlockStart(int position) throws Exception
      {
         return position;
      }
      
      public String toString()
      {
         return "FakeSequentialFile:" + this.fileName;
      }


   }

}
