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
package org.jboss.messaging.core.server.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;

/**
 * 
 * A ServerMessageImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerMessageImpl extends MessageImpl implements ServerMessage
{
   private long messageID;
   
   private long connectionID;
   
   private final AtomicInteger durableRefCount = new AtomicInteger(0);
              
   /*
    * Constructor for when reading from network
    */
   public ServerMessageImpl()
   {      
   }
   
   /*
    * Construct a MessageImpl from storage
    */
   public ServerMessageImpl(final long messageID)
   {
      super();
      
      this.messageID = messageID;      
   }
   
   /*
    * Copy constructor
    */
   public ServerMessageImpl(final ServerMessageImpl other)
   {
      super(other);
      
      this.messageID = other.messageID;
      
      this.connectionID = other.connectionID;
   }
   
   /**
    * Only used in testing
    */
   public ServerMessageImpl(final int type, final boolean durable, final long expiration,
                            final long timestamp, final byte priority)
   {
      super(type, durable, expiration, timestamp, priority);
   }
   
   public long getMessageID()
   {
      return messageID;
   }
   
   public void setMessageID(final long id)
   {
      this.messageID = id;
   }
   
   public long getConnectionID()
   {
      return connectionID;
   }
   
   public void setConnectionID(final long connectionID)
   {
      this.connectionID = connectionID;
   }
   
   public MessageReference createReference(final Queue queue)
   {
      MessageReference ref = new MessageReferenceImpl(this, queue);
      
      //references.add(ref);
      
      if (durable && queue.isDurable())
      {
         durableRefCount.incrementAndGet();
      }
      
      return ref;
   }
   
   public int getDurableRefCount()
   {
      return durableRefCount.get();
   }
   
   public int decrementDurableRefCount()
   {
      return durableRefCount.decrementAndGet();
   }
   
   public int incrementDurableRefCount()
   {
      return durableRefCount.incrementAndGet();
   }
     
   public ServerMessage copy()
   {
      return new ServerMessageImpl(this);
   }
   

}

