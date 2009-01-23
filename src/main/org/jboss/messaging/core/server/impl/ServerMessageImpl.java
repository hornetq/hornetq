/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;

/**
 * 
 * A ServerMessageImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class ServerMessageImpl extends MessageImpl implements ServerMessage
{
   private static final Logger log = Logger.getLogger(ServerMessageImpl.class);
   
   private final AtomicInteger durableRefCount = new AtomicInteger(0);

   /** Global reference counts for paging control */
   private final AtomicInteger refCount = new AtomicInteger(0);

   private volatile boolean stored;
   
   //We cache this
   private volatile int memoryEstimate = -1;
   
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
      super(messageID);
   }

   public ServerMessageImpl(final ServerMessageImpl other)
   {
      super(other);
   }

   /**
    * Only used in testing
    */
   public ServerMessageImpl(final byte type,
                            final boolean durable,
                            final long expiration,
                            final long timestamp,
                            final byte priority,
                            final MessagingBuffer buffer)
   {
      super(type, durable, expiration, timestamp, priority, buffer);
   }

   public void setMessageID(final long id)
   {
      messageID = id;
   }

   public MessageReference createReference(final Queue queue)
   {
      MessageReference ref = new MessageReferenceImpl(this, queue);

      return ref;
   }
   
   public boolean isStored()
   {
      return stored;
   }
   
   public void setStored()
   {
      stored = true;
   }
   
   public int incrementRefCount()
   {
      return refCount.incrementAndGet();
   }
   
   public int incrementDurableRefCount()
   {
      return durableRefCount.incrementAndGet();
   }

   public int decrementDurableRefCount()
   {
      return durableRefCount.decrementAndGet();
   }

   public int decrementRefCount()
   {
      return refCount.decrementAndGet();
   }
   
   public int getRefCount()
   {
      return refCount.get();
   }

   public boolean isLargeMessage()
   {
      return false;
   }

   public int getMemoryEstimate()
   {
      if (memoryEstimate == -1)
      {
         // This is just an estimate...
         // due to memory alignments and JVM implementation this could be very
         // different from reality
         memoryEstimate = getEncodeSize() + (16 + 4) * 2 + 1;
      }
      
      return memoryEstimate;
   }

   public ServerMessage copy()
   {
      ServerMessage m = new ServerMessageImpl(this);
      
      return m;
   }

   @Override
   public String toString()
   {
      return "ServerMessage[messageID=" + messageID +
             ", durable=" +
             durable +
             ", destination=" +
             getDestination() +
             "]";
   }
}
