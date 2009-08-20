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

package org.hornetq.tests.unit.core.postoffice.impl;

import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.transaction.xa.Xid;

import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.impl.BindingsImpl;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.Distributor;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * A BindingImplTest
 *
 * @author clebert
 * 
 * Created Mar 12, 2009 9:14:46 PM
 *
 *
 */
public class BindingImplTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRemoveWhileRouting() throws Exception
   {
      // It would require many iterations before getting a failure
      for (int i = 0; i < 500; i++)
      {
         internalTest(true);
      }
   }

   public void testRemoveWhileRedistributing() throws Exception
   {
      // It would require many iterations before getting a failure
      for (int i = 0; i < 500; i++)
      {
         internalTest(false);
      }
   }

   private void internalTest(final boolean route) throws Exception
   {
      final FakeBinding fake = new FakeBinding(new SimpleString("a"));

      final BindingsImpl bind = new BindingsImpl();
      bind.addBinding(fake);
      bind.addBinding(new FakeBinding(new SimpleString("a")));
      bind.addBinding(new FakeBinding(new SimpleString("a")));

      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               bind.removeBinding(fake);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };

      Queue queue = new FakeQueue(new SimpleString("a"));
      t.start();

      for (int i = 0; i < 100; i++)
      {
         if (route)
         {
            bind.route(new FakeMessage(), new FakeTransaction());
         }
         else
         {
            bind.redistribute(new FakeMessage(), queue, new FakeTransaction());
         }
      }
   }

   class FakeTransaction implements Transaction
   {

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#addOperation(org.hornetq.core.transaction.TransactionOperation)
       */
      public void addOperation(final TransactionOperation sync)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#commit()
       */
      public void commit() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#commit(boolean)
       */
      public void commit(final boolean onePhase) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getCreateTime()
       */
      public long getCreateTime()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getID()
       */
      public long getID()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getOperationsCount()
       */
      public int getOperationsCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getProperty(int)
       */
      public Object getProperty(final int index)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getState()
       */
      public State getState()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getXid()
       */
      public Xid getXid()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#markAsRollbackOnly(org.hornetq.core.exception.MessagingException)
       */
      public void markAsRollbackOnly(final MessagingException messagingException)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#prepare()
       */
      public void prepare() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#putProperty(int, java.lang.Object)
       */
      public void putProperty(final int index, final Object property)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#removeOperation(org.hornetq.core.transaction.TransactionOperation)
       */
      public void removeOperation(final TransactionOperation sync)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#resume()
       */
      public void resume()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#rollback()
       */
      public void rollback() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#setState(org.hornetq.core.transaction.Transaction.State)
       */
      public void setState(final State state)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#suspend()
       */
      public void suspend()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getDistinctQueues()
       */
      public Set<Queue> getDistinctQueues()
      {
         return Collections.emptySet();
      }

   }

   class FakeMessage implements ServerMessage
   {

      public Map<String, Object> toMap()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#copy(long)
       */
      public ServerMessage copy(final long newID) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#copy()
       */
      public ServerMessage copy() throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#createReference(org.hornetq.core.server.Queue)
       */
      public MessageReference createReference(final Queue queue)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#decrementDurableRefCount()
       */
      public int decrementDurableRefCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#decrementRefCount()
       */
      public int decrementRefCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#getMemoryEstimate()
       */
      public int getMemoryEstimate()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#getRefCount()
       */
      public int getRefCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#incrementDurableRefCount()
       */
      public int incrementDurableRefCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#incrementRefCount()
       */
      public int incrementRefCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#isLargeMessage()
       */
      public boolean isLargeMessage()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#isStored()
       */
      public boolean isStored()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#setMessageID(long)
       */
      public void setMessageID(final long id)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#setStored()
       */
      public void setStored()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#containsProperty(org.hornetq.utils.SimpleString)
       */
      public boolean containsProperty(final SimpleString key)
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#decode(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void decode(final MessagingBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#decodeBody(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void decodeBody(final MessagingBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#decodeProperties(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void decodeProperties(final MessagingBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encode(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void encode(final MessagingBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encodeBody(org.hornetq.core.remoting.spi.MessagingBuffer, long, int)
       */
      public void encodeBody(final MessagingBuffer buffer, final long start, final int size)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encodeBody(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void encodeBody(final MessagingBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encodeProperties(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void encodeProperties(final MessagingBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBody()
       */
      public MessagingBuffer getBody()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBodySize()
       */
      public int getBodySize()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getDestination()
       */
      public SimpleString getDestination()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getEncodeSize()
       */
      public int getEncodeSize()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getExpiration()
       */
      public long getExpiration()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getMessageID()
       */
      public long getMessageID()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getPriority()
       */
      public byte getPriority()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getProperties()
       */
      public TypedProperties getProperties()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getPropertiesEncodeSize()
       */
      public int getPropertiesEncodeSize()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getProperty(org.hornetq.utils.SimpleString)
       */
      public Object getProperty(final SimpleString key)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getPropertyNames()
       */
      public Set<SimpleString> getPropertyNames()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getTimestamp()
       */
      public long getTimestamp()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getType()
       */
      public byte getType()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#isDurable()
       */
      public boolean isDurable()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#isExpired()
       */
      public boolean isExpired()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putBooleanProperty(org.hornetq.utils.SimpleString, boolean)
       */
      public void putBooleanProperty(final SimpleString key, final boolean value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putByteProperty(org.hornetq.utils.SimpleString, byte)
       */
      public void putByteProperty(final SimpleString key, final byte value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putBytesProperty(org.hornetq.utils.SimpleString, byte[])
       */
      public void putBytesProperty(final SimpleString key, final byte[] value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putDoubleProperty(org.hornetq.utils.SimpleString, double)
       */
      public void putDoubleProperty(final SimpleString key, final double value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putFloatProperty(org.hornetq.utils.SimpleString, float)
       */
      public void putFloatProperty(final SimpleString key, final float value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putIntProperty(org.hornetq.utils.SimpleString, int)
       */
      public void putIntProperty(final SimpleString key, final int value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putLongProperty(org.hornetq.utils.SimpleString, long)
       */
      public void putLongProperty(final SimpleString key, final long value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putShortProperty(org.hornetq.utils.SimpleString, short)
       */
      public void putShortProperty(final SimpleString key, final short value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putStringProperty(org.hornetq.utils.SimpleString, org.hornetq.utils.SimpleString)
       */
      public void putStringProperty(final SimpleString key, final SimpleString value)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putTypedProperties(org.hornetq.utils.TypedProperties)
       */
      public void putTypedProperties(final TypedProperties properties)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#removeProperty(org.hornetq.utils.SimpleString)
       */
      public Object removeProperty(final SimpleString key)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setBody(org.hornetq.core.remoting.spi.MessagingBuffer)
       */
      public void setBody(final MessagingBuffer body)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setDestination(org.hornetq.utils.SimpleString)
       */
      public void setDestination(final SimpleString destination)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setDurable(boolean)
       */
      public void setDurable(final boolean durable)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setExpiration(long)
       */
      public void setExpiration(final long expiration)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setPriority(byte)
       */
      public void setPriority(final byte priority)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setTimestamp(long)
       */
      public void setTimestamp(final long timestamp)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBodyInputStream()
       */
      public InputStream getBodyInputStream()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#setBodyInputStream(java.io.InputStream)
       */
      public void setBodyInputStream(InputStream stream)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getLargeBodySize()
       */
      public long getLargeBodySize()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#containsProperty(java.lang.String)
       */
      public boolean containsProperty(String key)
      {
         
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getProperty(java.lang.String)
       */
      public Object getProperty(String key)
      {
         
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putBooleanProperty(java.lang.String, boolean)
       */
      public void putBooleanProperty(String key, boolean value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putByteProperty(java.lang.String, byte)
       */
      public void putByteProperty(String key, byte value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putBytesProperty(java.lang.String, byte[])
       */
      public void putBytesProperty(String key, byte[] value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putDoubleProperty(java.lang.String, double)
       */
      public void putDoubleProperty(String key, double value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putFloatProperty(java.lang.String, float)
       */
      public void putFloatProperty(String key, float value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putIntProperty(java.lang.String, int)
       */
      public void putIntProperty(String key, int value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putLongProperty(java.lang.String, long)
       */
      public void putLongProperty(String key, long value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putShortProperty(java.lang.String, short)
       */
      public void putShortProperty(String key, short value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putStringProperty(java.lang.String, java.lang.String)
       */
      public void putStringProperty(String key, String value)
      {
         
         
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#removeProperty(java.lang.String)
       */
      public Object removeProperty(String key)
      {
         
         return null;
      }

   }

   class FakeFilter implements Filter
   {

      /* (non-Javadoc)
       * @see org.hornetq.core.filter.Filter#getFilterString()
       */
      public SimpleString getFilterString()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.filter.Filter#match(org.hornetq.core.server.ServerMessage)
       */
      public boolean match(final ServerMessage message)
      {
         return false;
      }

   }

   class FakeBinding implements Binding
   {

      final SimpleString name;

      FakeBinding(final SimpleString name)
      {
         this.name = name;
      }

      public SimpleString getAddress()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getBindable()
       */
      public Bindable getBindable()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getClusterName()
       */
      public SimpleString getClusterName()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getDistance()
       */
      public int getDistance()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getFilter()
       */
      public Filter getFilter()
      {
         return new FakeFilter();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getID()
       */
      public int getID()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getRoutingName()
       */
      public SimpleString getRoutingName()
      {
         return name;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getType()
       */
      public BindingType getType()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#getUniqueName()
       */
      public SimpleString getUniqueName()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#isExclusive()
       */
      public boolean isExclusive()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#isHighAcceptPriority(org.hornetq.core.server.ServerMessage)
       */
      public boolean isHighAcceptPriority(final ServerMessage message)
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#setID(int)
       */
      public void setID(final int id)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.postoffice.Binding#willRoute(org.hornetq.core.server.ServerMessage)
       */
      public void willRoute(final ServerMessage message)
      {

      }

   }

   class FakeQueue implements Queue
   {

      private SimpleString name;
      
      FakeQueue(SimpleString name)
      {
         this.name = name;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.server.MessageReference)
       */
      public void acknowledge(MessageReference ref) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#acknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void acknowledge(Transaction tx, MessageReference ref) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#activate()
       */
      public boolean activate()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#activateNow(java.util.concurrent.Executor)
       */
      public void activateNow(Executor executor)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addConsumer(org.hornetq.core.server.Consumer)
       */
      public void addConsumer(Consumer consumer) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addFirst(org.hornetq.core.server.MessageReference)
       */
      public void addFirst(MessageReference ref)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addLast(org.hornetq.core.server.MessageReference)
       */
      public void addLast(MessageReference ref)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#addRedistributor(long, java.util.concurrent.Executor, org.hornetq.core.remoting.Channel)
       */
      public void addRedistributor(long delay, Executor executor, Channel replicatingChannel)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void cancel(Transaction tx, MessageReference ref) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#cancel(org.hornetq.core.server.MessageReference)
       */
      public void cancel(MessageReference reference) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#cancelRedistributor()
       */
      public void cancelRedistributor() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#changeReferencePriority(long, byte)
       */
      public boolean changeReferencePriority(long messageID, byte newPriority) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#checkDLQ(org.hornetq.core.server.MessageReference)
       */
      public boolean checkDLQ(MessageReference ref) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#consumerFailedOver()
       */
      public boolean consumerFailedOver()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deleteAllReferences()
       */
      public int deleteAllReferences() throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deleteMatchingReferences(org.hornetq.core.filter.Filter)
       */
      public int deleteMatchingReferences(Filter filter) throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deleteReference(long)
       */
      public boolean deleteReference(long messageID) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deliverAsync(java.util.concurrent.Executor)
       */
      public void deliverAsync(Executor executor)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#deliverNow()
       */
      public void deliverNow()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expire(org.hornetq.core.server.MessageReference)
       */
      public void expire(MessageReference ref) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expireReference(long)
       */
      public boolean expireReference(long messageID) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expireReferences(org.hornetq.core.filter.Filter)
       */
      public int expireReferences(Filter filter) throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#expireReferences()
       */
      public void expireReferences() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getConsumerCount()
       */
      public int getConsumerCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getConsumers()
       */
      public Set<Consumer> getConsumers()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getDeliveringCount()
       */
      public int getDeliveringCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getDistributionPolicy()
       */
      public Distributor getDistributionPolicy()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getFilter()
       */
      public Filter getFilter()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getMessageCount()
       */
      public int getMessageCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getMessagesAdded()
       */
      public int getMessagesAdded()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getName()
       */
      public SimpleString getName()
      {
         return name;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getPersistenceID()
       */
      public long getPersistenceID()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getReference(long)
       */
      public MessageReference getReference(long id)
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getScheduledCount()
       */
      public int getScheduledCount()
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#getScheduledMessages()
       */
      public List<MessageReference> getScheduledMessages()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isBackup()
       */
      public boolean isBackup()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isDurable()
       */
      public boolean isDurable()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#isTemporary()
       */
      public boolean isTemporary()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#list(org.hornetq.core.filter.Filter)
       */
      public List<MessageReference> list(Filter filter)
      {

         return null;
      }
      
      public Iterator<MessageReference> iterator()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#moveReference(long, org.hornetq.utils.SimpleString)
       */
      public boolean moveReference(long messageID, SimpleString toAddress) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#moveReferences(org.hornetq.core.filter.Filter, org.hornetq.utils.SimpleString)
       */
      public int moveReferences(Filter filter, SimpleString toAddress) throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#reacknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void reacknowledge(Transaction tx, MessageReference ref) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#referenceHandled()
       */
      public void referenceHandled()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#removeConsumer(org.hornetq.core.server.Consumer)
       */
      public boolean removeConsumer(Consumer consumer) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#removeFirstReference(long)
       */
      public MessageReference removeFirstReference(long id) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#removeReferenceWithID(long)
       */
      public MessageReference removeReferenceWithID(long id) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#reroute(org.hornetq.core.server.ServerMessage, org.hornetq.core.transaction.Transaction)
       */
      public MessageReference reroute(ServerMessage message, Transaction tx) throws Exception
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#sendMessageToDeadLetterAddress(long)
       */
      public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#setBackup()
       */
      public void setBackup()
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#setDistributionPolicy(org.hornetq.core.server.Distributor)
       */
      public void setDistributionPolicy(Distributor policy)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#setPersistenceID(long)
       */
      public void setPersistenceID(long id)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Bindable#preroute(org.hornetq.core.server.ServerMessage, org.hornetq.core.transaction.Transaction)
       */
      public void preroute(ServerMessage message, Transaction tx) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Bindable#route(org.hornetq.core.server.ServerMessage, org.hornetq.core.transaction.Transaction)
       */
      public void route(ServerMessage message, Transaction tx) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#lock()
       */
      public void lockDelivery()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.Queue#unlock()
       */
      public void unlockDelivery()
      {
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
