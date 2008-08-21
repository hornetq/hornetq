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
package org.jboss.messaging.core.remoting.impl.invm;

import java.nio.ByteBuffer;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingHandler;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.ConnectionLifeCycleListener;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * A InVMConnection
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InVMConnection implements Connection
{
   private static final Logger log = Logger.getLogger(InVMConnection.class);
   
   private final RemotingHandler handler;
   
   private final ConnectionLifeCycleListener listener;
   
   private final String id;
   
   private volatile boolean started;
   
   public InVMConnection(final RemotingHandler handler, final ConnectionLifeCycleListener listener)
   {
      this (UUIDGenerator.getInstance().generateSimpleStringUUID().toString(), handler, listener);
   }
   
   public InVMConnection(final String id, final RemotingHandler handler, final ConnectionLifeCycleListener listener)
   {
      this.handler = handler;
      
      this.listener = listener;
      
      this.id = id;
      
      listener.connectionCreated(this);
      
      started = true;
   }

   public void close()
   {      
      if (!started)
      {
         return;
      }
      
      listener.connectionDestroyed(id);
      
      started = false;
   }

   public MessagingBuffer createBuffer(int size)
   {
      return new ByteBufferWrapper(ByteBuffer.allocate(size));
   }

   public Object getID()
   {
      return id;
   }

   public void write(MessagingBuffer buffer)
   {
      try
      {
         buffer.getInt(); //read and discard
         handler.bufferReceived(id, buffer);
      }
      catch (Exception e)
      {
         final String msg = "Failed to write to handler";
         log.error(msg, e);
         throw new IllegalStateException(msg, e);         
      }
   }

}
