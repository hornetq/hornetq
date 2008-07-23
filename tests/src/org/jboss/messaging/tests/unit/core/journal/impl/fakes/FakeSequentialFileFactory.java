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
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class FakeSequentialFileFactory implements SequentialFileFactory
{
   private static final Logger log = Logger.getLogger(FakeSequentialFileFactory.class);
      
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private final Map<String, FakeSequentialFile> fileMap = new ConcurrentHashMap<String, FakeSequentialFile>();
   
   private final int alignment;
   
   private final boolean supportsCallback; 
   
   private volatile boolean holdCallbacks;
   
   private final List<Runnable> callbacksInHold;
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public FakeSequentialFileFactory(final int alignment, final boolean supportsCallback)
   {
      this.alignment = alignment;
      this.supportsCallback = supportsCallback;
      callbacksInHold = new ArrayList<Runnable>();
   }

   public FakeSequentialFileFactory()
   {
      this(1, false);
   }

   
   
   // Public --------------------------------------------------------
   
   public SequentialFile createSequentialFile(final String fileName, final int maxAIO, final long timeout) throws Exception
   {
      FakeSequentialFile sf = fileMap.get(fileName);
      
      if (sf == null)
      {                 
         sf = newSequentialFile(fileName);
         
         fileMap.put(fileName, sf);
      }
      else
      { 
          sf.getData().position(0);
         
         //log.debug("positioning data to 0");
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
   
   public boolean isSupportsCallbacks()
   {
      return supportsCallback;
   }
   
   public ByteBuffer newBuffer(int size)
   {
      if (size % alignment != 0)
      {
         size = (size / alignment + 1) * alignment;
      }
      return ByteBuffer.allocateDirect(size);
   }
   
   public ByteBuffer wrapBuffer(byte[] bytes)
   {
      return ByteBuffer.wrap(bytes);
   }
   
   public boolean isHoldCallbacks()
   {
      return holdCallbacks;
   }

   public void setHoldCallbacks(boolean holdCallbacks)
   {
      this.holdCallbacks = holdCallbacks;
   }

   public void flushAllCallbacks()
   {
      for (Runnable action : callbacksInHold)
      {
         action.run();
      }
      
      callbacksInHold.clear();
   }

   public void flushCallback(int position)
   {
      Runnable run = callbacksInHold.get(position);
      run.run();
      callbacksInHold.remove(run);
   }
   
   public int getNumberOfCallbacks()
   {
      return callbacksInHold.size();
   }
   
   public int getAlignment()
   {
      return alignment;
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected FakeSequentialFile newSequentialFile(final String fileName)
   {
      return new FakeSequentialFile(fileName);
   }
   
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   public class FakeSequentialFile implements SequentialFile
   {
      private volatile boolean open;
      
      private final String fileName;
      
      private ByteBuffer data;
      
      public ByteBuffer getData()
      {
         return data;
      }
      
      public boolean isOpen()
      {
         //log.debug("is open" + System.identityHashCode(this) +" open is now " + open);
         return open;
      }
      
      public FakeSequentialFile(final String fileName)
      {
         this.fileName = fileName;   
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

      public void fill(final int pos, final int size, final byte fillCharacter) throws Exception
      {     
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         checkAndResize(pos + size);
         
         //log.debug("size is " + size + " pos is " + pos);
         
         for (int i = pos; i < size + pos; i++)
         {
            data.array()[i] = fillCharacter;
            
            //log.debug("Filling " + pos + " with char " + fillCharacter);
         }                 
      }
      
      public int read(final ByteBuffer bytes) throws Exception
      {
         return read(bytes, null);
      }
      
      public int read(final ByteBuffer bytes, final IOCallback callback) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         byte[] bytesRead = new byte[bytes.limit()];
         
         data.get(bytesRead);
         
         bytes.put(bytesRead);
         
         bytes.rewind();
         
         if (callback != null) callback.done();
         
         return bytesRead.length;
      }

      public void position(final int pos) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         checkAlignment(pos);
         
         data.position(pos);
      }

      public int write(final ByteBuffer bytes, final IOCallback callback) throws Exception
      {
         if (!open)
         {
            throw new IllegalStateException("Is closed");
         }
         
         final int position = data == null ? 0 : data.position();
         
         checkAlignment(position);
         
         checkAlignment(bytes.limit());
         
         checkAndResize(bytes.capacity() + position);
         
         Runnable action = new Runnable()
         {

            public void run()
            {
               
               data.put(bytes);
               
               if (callback!=null) callback.done();
            }
            
         };
         
         if (holdCallbacks && callback != null)
         {
            FakeSequentialFileFactory.this.callbacksInHold.add(action);
         }
         else
         {
            action.run();
         }
         
         return bytes.limit();
         
      }
      
      public int write(final ByteBuffer bytes, final boolean sync) throws Exception
      {
         return write(bytes, null);
      }
      
      private void checkAndResize(final int size)
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
         return alignment;
      }

      public int calculateBlockStart(final int position) throws Exception
      {
         int pos = ((position / alignment) + (position % alignment != 0 ? 1 : 0)) * alignment;
         
         return pos;
      }
      
      public String toString()
      {
         return "FakeSequentialFile:" + this.fileName;
      }
      
      private void checkAlignment (final int position)
      {
         if ( position % alignment != 0)
         {
            throw new IllegalStateException("Position is not aligned to " + alignment);
         }
      }


   }

}
