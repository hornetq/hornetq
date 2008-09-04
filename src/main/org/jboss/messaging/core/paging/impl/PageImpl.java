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


package org.jboss.messaging.core.paging.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageMessage;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.util.VariableLatch;

/**
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PageImpl implements Page
{
   
   // Constants -----------------------------------------------------
   
   private static final int SIZE_INTEGER = 4;
   
   private static final int SIZE_BYTE = 1;
   
   public static final int SIZE_RECORD = SIZE_BYTE + SIZE_INTEGER + SIZE_BYTE; 
   
   public static final byte START_BYTE= (byte)'{';
   public static final byte END_BYTE= (byte)'}';
   
   // Attributes ----------------------------------------------------
   
   private final int pageId;
   private final AtomicInteger numberOfMessages = new AtomicInteger(0);
   private final SequentialFile file;
   private final SequentialFileFactory fileFactory;
   private final PagingCallback callback;
   private final AtomicInteger size = new AtomicInteger(0);
   
   // Static --------------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public PageImpl(final SequentialFileFactory factory, final SequentialFile file,final int pageId) throws Exception
   {
      this.pageId = pageId;
      this.file = file;
      this.fileFactory = factory;
      if (factory.isSupportsCallbacks())
      {
         callback = new PagingCallback();
      }
      else
      {
         callback = null;
      }
   }
   
   
   // Public --------------------------------------------------------

   
   // PagingFile implementation
   
   
   public int getPageId()
   {
      return pageId;
   }
   
   public PageMessage[] read() throws Exception
   {
      
      ArrayList<PageMessage> messages = new ArrayList<PageMessage>();

      ByteBuffer buffer = fileFactory.newBuffer((int)file.size());
      file.position(0);
      file.read(buffer);
      
      ByteBufferWrapper messageBuffer = new ByteBufferWrapper(buffer);
      
      while (buffer.hasRemaining())
      {
         final int position = buffer.position();
         
         byte byteRead = buffer.get();
         
         if (byteRead == START_BYTE)
         {
            if (buffer.position() + SIZE_INTEGER < buffer.limit())
            {
               int messageSize = buffer.getInt();
               int oldPos = buffer.position();
               if (buffer.position() + messageSize < buffer.limit() && buffer.get(oldPos + messageSize) == END_BYTE)
               {
                  PageMessage msg = instantiateObject();
                  msg.decode(messageBuffer);
                  messages.add(msg);
               }
               else
               {
                  buffer.position(position + 1); 
               }
            }
         }
         else
         {
            buffer.position(position + 1); 
         }
      }
      
      numberOfMessages.set(messages.size());
      
      return messages.toArray(instantiateArray(messages.size()));
   }
   
   public void write(final PageMessage message) throws Exception
   {
      ByteBuffer buffer = fileFactory.newBuffer(message.getEncodeSize() + SIZE_RECORD);
      buffer.put(START_BYTE);
      buffer.putInt(message.getEncodeSize());
      message.encode(new ByteBufferWrapper(buffer));
      buffer.put(END_BYTE);
      buffer.rewind();

      if (callback != null)
      {
         callback.countUp();
         file.write(buffer, callback);
      }
      else
      {
         file.write(buffer, false);
      }
      
      numberOfMessages.incrementAndGet();
      size.addAndGet(buffer.limit());
      
   }
   
   public void sync() throws Exception
   {
      if (callback != null)
      {
         callback.waitCompletion();
      }
      else
      {
         file.sync();
      }
   }
   
   public void open() throws Exception
   {
      file.open();
      this.size.set((int)file.size());
      file.position(0);
   }
   
   public void close() throws Exception
   {
      file.close();
   }
   
   public void delete() throws Exception
   {
      file.delete();
   }
   
   public int getNumberOfMessages()
   {
      return numberOfMessages.intValue();
   }
   
   public int getSize()
   {
      return this.size.intValue();
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected  PageMessage instantiateObject()
   {
      return new PageMessageImpl();
   }

   
   protected PageMessage[] instantiateArray(final int size)
   {
      return new PageMessage[size];
   }
   
   // Private -------------------------------------------------------
   
   
   // Inner classes -------------------------------------------------

   private static class PagingCallback implements IOCallback
   {      
      private final VariableLatch countLatch = new VariableLatch();
      
      private volatile String errorMessage = null;
      
      private volatile int errorCode = 0;
      
      public void countUp()
      {
         countLatch.up();
      }
      
      public void done()
      {
         countLatch.down();
      }
      
      public void waitCompletion() throws InterruptedException
      {
         countLatch.waitCompletion();
         
         if (errorMessage != null)
         {
            throw new IllegalStateException("Error on Callback: " + errorCode + " - " + errorMessage);
         }
      }
      
      public void onError(final int errorCode, final String errorMessage)
      {
         this.errorMessage = errorMessage;
         this.errorCode = errorCode;
         countLatch.down();
      }
      
   }
   
}
