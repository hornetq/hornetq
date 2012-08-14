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
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.impl.BindingsImpl;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

/**
 * A BindingImplTest
 *
 * @author clebert
 *
 * Created Mar 12, 2009 9:14:46 PM
 *
 *
 */
public class BindingsImplTest extends UnitTestCase
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

      final BindingsImpl bind = new BindingsImpl(null, null, null);
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
            bind.route(new ServerMessageImpl(i, 100), new RoutingContextImpl(new FakeTransaction()));
         }
         else
         {
            bind.redistribute(new ServerMessageImpl(i, 100), queue, new RoutingContextImpl(new FakeTransaction()));
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

      public boolean hasTimedOut(long currentTime, int defaultTimeout)
      {
         return false;
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
       * @see org.hornetq.core.transaction.Transaction#markAsRollbackOnly(org.hornetq.api.core.exception.HornetQException)
       */
      public void markAsRollbackOnly(final HornetQException exception)
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

      public void setContainsPersistent()
      {


      }

      public void setTimeout(int timeout)
      {

      }

      public Transaction copy()
      {
         return null;
      }

      public void afterCommit()
      {
      }

      public void afterPrepare()
      {
      }

      public void afterRollback()
      {
      }

      public void beforeCommit() throws Exception
      {
      }

      public void beforePrepare() throws Exception
      {
      }

      public void beforeRollback() throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#isContainsPersistent()
       */
      public boolean isContainsPersistent()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#getAllOperations()
       */
      public List<TransactionOperation> getAllOperations()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#isWaitBeforeCommit()
       */
      public boolean isWaitBeforeCommit()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.Transaction#setWaitBeforeCommit(boolean)
       */
      public void setWaitBeforeCommit(boolean waitBeforeCommit)
      {


      }

   }

   class FakeMessage implements ServerMessage
   {

      public ServerMessage copy()
      {

         return null;
      }

      public ServerMessage copy(final long newID)
      {

         return null;
      }

      public MessageReference createReference(final Queue queue)
      {

         return null;
      }

      public int decrementDurableRefCount()
      {

         return 0;
      }

      public int decrementRefCount() throws Exception
      {

         return 0;
      }

      public void encodeMessageIDToBuffer()
      {


      }

      public int getMemoryEstimate()
      {

         return 0;
      }

      public PagingStore getPagingStore()
      {

         return null;
      }

      public int getRefCount()
      {

         return 0;
      }

      public int incrementDurableRefCount()
      {

         return 0;
      }

      public int incrementRefCount() throws Exception
      {

         return 0;
      }

      public ServerMessage makeCopyForExpiryOrDLA(final long newID, final boolean expiry) throws Exception
      {

         return null;
      }

      public boolean page() throws Exception
      {

         return false;
      }

      public boolean page(final long transactionID) throws Exception
      {

         return false;
      }

      public void setMessageID(final long id)
      {


      }

      public void setOriginalHeaders(final ServerMessage other, final boolean expiry)
      {


      }

      public void setPagingStore(final PagingStore store)
      {


      }

      public boolean storeIsPaging()
      {

         return false;
      }

      public void bodyChanged()
      {


      }

      public void checkCopy()
      {


      }

      public boolean containsProperty(final SimpleString key)
      {

         return false;
      }

      public boolean containsProperty(final String key)
      {

         return false;
      }

      public void decodeFromBuffer(final HornetQBuffer buffer)
      {


      }

      public void decodeHeadersAndProperties(final HornetQBuffer buffer)
      {


      }

      public void encodeHeadersAndProperties(final HornetQBuffer buffer)
      {


      }

      public HornetQBuffer getBodyBuffer()
      {

         return null;
      }

      public HornetQBuffer getBodyBufferCopy()
      {
         return null;
      }

      public BodyEncoder getBodyEncoder() throws HornetQException
      {

         return null;
      }

      public InputStream getBodyInputStream()
      {

         return null;
      }

      public Boolean getBooleanProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Boolean getBooleanProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Byte getByteProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Byte getByteProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public byte[] getBytesProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public byte[] getBytesProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public SimpleString getAddress()
      {

         return null;
      }

      public Double getDoubleProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Double getDoubleProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public HornetQBuffer getEncodedBuffer()
      {

         return null;
      }

      public int getEncodeSize()
      {

         return 0;
      }

      public int getEndOfBodyPosition()
      {

         return 0;
      }

      public int getEndOfMessagePosition()
      {

         return 0;
      }

      public long getExpiration()
      {

         return 0;
      }

      public Float getFloatProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Float getFloatProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public int getHeadersAndPropertiesEncodeSize()
      {

         return 0;
      }

      public Integer getIntProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Integer getIntProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Long getLongProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Long getLongProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public long getMessageID()
      {

         return 0;
      }

      public Object getObjectProperty(final SimpleString key)
      {

         return null;
      }

      public Object getObjectProperty(final String key)
      {

         return null;
      }

      public byte getPriority()
      {

         return 0;
      }

      public TypedProperties getProperties()
      {

         return null;
      }

      public Set<SimpleString> getPropertyNames()
      {

         return null;
      }

      public Short getShortProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public Short getShortProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public SimpleString getSimpleStringProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public SimpleString getSimpleStringProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public String getStringProperty(final SimpleString key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public String getStringProperty(final String key) throws HornetQPropertyConversionException
      {

         return null;
      }

      public long getTimestamp()
      {

         return 0;
      }

      public byte getType()
      {

         return 0;
      }

      public HornetQBuffer getWholeBuffer()
      {

         return null;
      }

      public boolean isDurable()
      {

         return false;
      }

      public boolean isExpired()
      {

         return false;
      }

      public boolean isLargeMessage()
      {

         return false;
      }

      public void putBooleanProperty(final SimpleString key, final boolean value)
      {


      }

      public void putBooleanProperty(final String key, final boolean value)
      {


      }

      public void putByteProperty(final SimpleString key, final byte value)
      {


      }

      public void putByteProperty(final String key, final byte value)
      {


      }

      public void putBytesProperty(final SimpleString key, final byte[] value)
      {


      }

      public void putBytesProperty(final String key, final byte[] value)
      {


      }

      public void putDoubleProperty(final SimpleString key, final double value)
      {


      }

      public void putDoubleProperty(final String key, final double value)
      {


      }

      public void putFloatProperty(final SimpleString key, final float value)
      {


      }

      public void putFloatProperty(final String key, final float value)
      {


      }

      public void putIntProperty(final SimpleString key, final int value)
      {

      }

      public void putIntProperty(final String key, final int value)
      {

      }

      public void putLongProperty(final SimpleString key, final long value)
      {

      }

      public void putLongProperty(final String key, final long value)
      {

      }

      public void putObjectProperty(final SimpleString key, final Object value) throws HornetQPropertyConversionException
      {

      }

      public void putObjectProperty(final String key, final Object value) throws HornetQPropertyConversionException
      {

      }

      public void putShortProperty(final SimpleString key, final short value)
      {

      }

      public void putShortProperty(final String key, final short value)
      {

      }

      public void putStringProperty(final SimpleString key, final SimpleString value)
      {

      }

      public void putStringProperty(final String key, final String value)
      {

      }

      public void putTypedProperties(final TypedProperties properties)
      {

      }

      public Object removeProperty(final SimpleString key)
      {
         return null;
      }

      public Object removeProperty(final String key)
      {
         return null;
      }

      public void resetCopied()
      {

      }

      public void setAddress(final SimpleString destination)
      {

      }

      public void setDurable(final boolean durable)
      {

      }

      public void setExpiration(final long expiration)
      {

      }

      public void setPriority(final byte priority)
      {

      }

      public void setTimestamp(final long timestamp)
      {

      }

      public Map<String, Object> toMap()
      {
         return null;
      }

      public void decode(final HornetQBuffer buffer)
      {

      }

      public void encode(final HornetQBuffer buffer)
      {

      }

      public void setAddressTransient(SimpleString address)
      {

      }

      public UUID getUserID()
      {
         return null;
      }

      public void setUserID(UUID userID)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.impl.MessageInternal#isServerMessage()
       */
      public boolean isServerMessage()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.impl.MessageInternal#getTypedProperties()
       */
      public TypedProperties getTypedProperties()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#getDuplicateIDBytes()
       */
      public byte[] getDuplicateIDBytes()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#getDuplicateProperty()
       */
      public Object getDuplicateProperty()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#hasInternalProperties()
       */
      public boolean hasInternalProperties()
      {
         return false;
      }

      public void finishCopy() throws Exception
      {
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

      public void close() throws Exception
      {


      }

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

      public long getID()
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

      public boolean isExclusive()
      {
         return false;
      }

      public boolean isHighAcceptPriority(final ServerMessage message)
      {
         return false;
      }

      public void route(final ServerMessage message, final RoutingContext context) throws Exception
      {

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
