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

package org.jboss.messaging.core.client.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.jboss.messaging.core.client.ClientFileMessage;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * A ClientFileMessageImpl
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Oct 13, 2008 4:33:56 PM
 *
 *
 */
public class ClientFileMessageImpl extends ClientMessageImpl implements ClientFileMessage
{
   private File file;

   private FileChannel currentChannel;

   public ClientFileMessageImpl()
   {
   }

   public ClientFileMessageImpl(final boolean durable)
   {
      super(durable, null);
   }

   /**
    * @param type
    * @param durable
    * @param expiration
    * @param timestamp
    * @param priority
    * @param body
    */
   public ClientFileMessageImpl(final byte type,
                                final boolean durable,
                                final long expiration,
                                final long timestamp,
                                final byte priority,
                                final MessagingBuffer body)
   {
      super(type, durable, expiration, timestamp, priority, body);
   }

   /**
    * @param type
    * @param durable
    * @param body
    */
   public ClientFileMessageImpl(final byte type, final boolean durable, final MessagingBuffer body)
   {
      super(type, durable, body);
   }

   /**
    * @param deliveryCount
    */
   public ClientFileMessageImpl(final int deliveryCount)
   {
      super(deliveryCount);
   }

   /**
    * @return the file
    */
   public File getFile()
   {
      return file;
   }

   /**
    * @param file the file to set
    */
   public void setFile(final File file)
   {
      this.file = file;
   }

   @Override
   public MessagingBuffer getBody()
   {
      throw new UnsupportedOperationException("getBody is not supported on FileMessages.");
   }

   /**
    * If a ClientFileMessage is Smaller then the MinLargeMessage configured on the SessionFactory (or JMSConnectionFactory), it will still be sent as any other message,
    * and for that the file body (which should be small) will be read from the file an populated on the output buffer
    *  
    *  */
   public void encodeBody(MessagingBuffer buffer)
   {
      FileChannel channel = null;
      try
      {
         // We open a new channel on getBody.
         // for a better performance, users should be using the channels when using file
         channel = newChannel();

         ByteBuffer fileBuffer = ByteBuffer.allocate((int)channel.size());

         channel.position(0);
         channel.read(fileBuffer);

         buffer.putBytes(fileBuffer.array(), 0, fileBuffer.limit());
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
      finally
      {
         try
         {
            channel.close();
         }
         catch (Throwable ignored)
         {

         }
      }
   }

   /** 
    * Read the file content from start to size.
    */
   @Override
   public synchronized void encodeBody(final MessagingBuffer buffer, final long start, final int size)
   {
      try
      {
         FileChannel channel = getChannel();

         ByteBuffer bufferRead = ByteBuffer.allocate(size);

         channel.position(start);
         channel.read(bufferRead);

         buffer.putBytes(bufferRead.array());
      }
      catch (Exception e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }
   }

   @Override
   public void setBody(final MessagingBuffer body)
   {
      throw new RuntimeException("Not supported");
   }

   public synchronized FileChannel getChannel() throws MessagingException
   {
      if (currentChannel == null)
      {
         currentChannel = newChannel();
      }

      return currentChannel;
   }

   public synchronized void closeChannel() throws MessagingException
   {
      if (currentChannel != null)
      {
         try
         {
            currentChannel.close();
         }
         catch (IOException e)
         {
            throw new MessagingException(MessagingException.INTERNAL_ERROR, e.getMessage(), e);
         }
         currentChannel = null;
      }

   }

   @Override
   public synchronized int getBodySize()
   {
      return (int)file.length();
   }

   /**
    * @return
    * @throws FileNotFoundException
    * @throws IOException
    */
   private FileChannel newChannel() throws MessagingException
   {
      try
      {
         RandomAccessFile randomFile = new RandomAccessFile(getFile(), "rw");
         
         randomFile.seek(0);

         FileChannel channel = randomFile.getChannel();
         
         return channel;
      }
      catch (IOException e)
      {
         throw new MessagingException(MessagingException.INTERNAL_ERROR, e.getMessage(), e);
      }
   }

}
