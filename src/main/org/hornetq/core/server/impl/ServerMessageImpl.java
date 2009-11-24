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

package org.hornetq.core.server.impl;

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.SimpleString;

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

   // We cache this
   private volatile int memoryEstimate = -1;

   private PagingStore pagingStore;

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

   public ServerMessageImpl(final ServerMessage other)
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
                            final HornetQBuffer buffer)
   {
      super(type, durable, expiration, timestamp, priority, buffer);
   }

   public void setMessageID(final long id)
   {
      messageID = id;
   }

   public void setType(final byte type)
   {
      this.type = type;
   }

   public MessageReference createReference(final Queue queue)
   {
      MessageReference ref = new MessageReferenceImpl(this, queue);

      return ref;
   }

   public int incrementRefCount(final MessageReference reference) throws Exception
   {
      int count = refCount.incrementAndGet();

      if (pagingStore != null)
      {
         if (count == 1)
         {
            pagingStore.addSize(this, true);
         }

         pagingStore.addSize(reference, true);
      }

      return count;
   }

   public int decrementRefCount(final MessageReference reference) 
   {
      int count = refCount.decrementAndGet();

      if (pagingStore != null)
      {
         if (count == 0)
         {
            pagingStore.addSize(this, false);
         }

         pagingStore.addSize(reference, false);
      }

      return count;
   }

   public int incrementDurableRefCount()
   {
      return durableRefCount.incrementAndGet();
   }

   public int decrementDurableRefCount()
   {
      return durableRefCount.decrementAndGet();
   }

   public int getRefCount()
   {
      return refCount.get();
   }

   public boolean isLargeMessage()
   {
      return false;
   }

   public long getLargeBodySize()
   {
      return getBodySize();
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

   public ServerMessage copy(final long newID) throws Exception
   {
      ServerMessage m = new ServerMessageImpl(this);

      m.setMessageID(newID);

      return m;
   }

   public ServerMessage copy() throws Exception
   {
      return new ServerMessageImpl(this);
   }

   public ServerMessage makeCopyForExpiryOrDLA(final long newID, final boolean expiry) throws Exception
   {
      /*
       We copy the message and send that to the dla/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message destination, expiry time
       and original message id
      */

      ServerMessage copy = copy(newID);

      copy.setOriginalHeaders(this, expiry);

      return copy;
   }

   public void setOriginalHeaders(final ServerMessage other, final boolean expiry)
   {
      if (other.containsProperty(HDR_ORIG_MESSAGE_ID))
      {
         putStringProperty(HDR_ORIGINAL_DESTINATION, other.getSimpleStringProperty(HDR_ORIGINAL_DESTINATION));

         putLongProperty(HDR_ORIG_MESSAGE_ID, other.getLongProperty(HDR_ORIG_MESSAGE_ID));
      }
      else
      {
         SimpleString originalQueue = other.getDestination();

         putStringProperty(HDR_ORIGINAL_DESTINATION, originalQueue);

         putLongProperty(HDR_ORIG_MESSAGE_ID, other.getMessageID());
      }

      // reset expiry
      setExpiration(0);

      if (expiry)
      {
         long actualExpiryTime = System.currentTimeMillis();

         putLongProperty(HDR_ACTUAL_EXPIRY_TIME, actualExpiryTime);
      }
   }

   public void setPagingStore(final PagingStore pagingStore)
   {
      this.pagingStore = pagingStore;

      // On the server side, we reset the address to point to the instance of address in the paging store
      // Otherwise each message would have its own copy of the address String which would take up more memory
      destination = pagingStore.getAddress();
   }

   public PagingStore getPagingStore()
   {
      return pagingStore;
   }

   public boolean page(final boolean duplicateDetection) throws Exception
   {
      if (pagingStore != null)
      {
         return pagingStore.page(this, duplicateDetection);
      }
      else
      {
         return false;
      }
   }

   public boolean page(final long transactionID, final boolean duplicateDetection) throws Exception
   {
      if (pagingStore != null)
      {
         return pagingStore.page(this, transactionID, duplicateDetection);
      }
      else
      {
         return false;
      }
   }

   public boolean storeIsPaging()
   {
      if (pagingStore != null)
      {
         return pagingStore.isPaging();
      }
      else
      {
         return false;
      }
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
