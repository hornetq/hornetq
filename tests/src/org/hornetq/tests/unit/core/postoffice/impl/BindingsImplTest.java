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
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.PropertyConversionException;
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

      final BindingsImpl bind = new BindingsImpl(null);
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
       * @see org.hornetq.core.transaction.Transaction#markAsRollbackOnly(org.hornetq.core.exception.HornetQException)
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
         // TODO Auto-generated method stub
         
      }

   }


   class FakeMessage implements ServerMessage
   {

      public ServerMessage copy()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public ServerMessage copy(long newID)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public MessageReference createReference(Queue queue)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int decrementDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int decrementRefCount(MessageReference reference) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public void encodeMessageIDToBuffer()
      {
         // TODO Auto-generated method stub
         
      }

      public int getMemoryEstimate()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public PagingStore getPagingStore()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int incrementDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int incrementRefCount(MessageReference reference) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public ServerMessage makeCopyForExpiryOrDLA(long newID, boolean expiry) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean page(boolean duplicateDetection) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean page(long transactionID, boolean duplicateDetection) throws Exception
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void setMessageID(long id)
      {
         // TODO Auto-generated method stub
         
      }

      public void setOriginalHeaders(ServerMessage other, boolean expiry)
      {
         // TODO Auto-generated method stub
         
      }

      public void setPagingStore(PagingStore store)
      {
         // TODO Auto-generated method stub
         
      }

      public boolean storeIsPaging()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void bodyChanged()
      {
         // TODO Auto-generated method stub
         
      }

      public void checkCopy()
      {
         // TODO Auto-generated method stub
         
      }

      public boolean containsProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean containsProperty(String key)
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void decodeFromBuffer(HornetQBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void decodeHeadersAndProperties(HornetQBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void encodeHeadersAndProperties(HornetQBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public HornetQBuffer getBodyBuffer()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public BodyEncoder getBodyEncoder() throws HornetQException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public InputStream getBodyInputStream()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Boolean getBooleanProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Boolean getBooleanProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Byte getByteProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Byte getByteProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public byte[] getBytesProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public byte[] getBytesProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public SimpleString getDestination()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Double getDoubleProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Double getDoubleProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public HornetQBuffer getEncodedBuffer()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getEncodeSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getEndOfBodyPosition()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int getEndOfMessagePosition()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public long getExpiration()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public Float getFloatProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Float getFloatProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int getHeadersAndPropertiesEncodeSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public Integer getIntProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Integer getIntProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Long getLongProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Long getLongProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public long getMessageID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public Object getObjectProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Object getObjectProperty(String key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public byte getPriority()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public TypedProperties getProperties()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Set<SimpleString> getPropertyNames()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Short getShortProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Short getShortProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public SimpleString getSimpleStringProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public SimpleString getSimpleStringProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getStringProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public String getStringProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      public long getTimestamp()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public byte getType()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public HornetQBuffer getWholeBuffer()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public boolean isDurable()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isExpired()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public boolean isLargeMessage()
      {
         // TODO Auto-generated method stub
         return false;
      }

      public void putBooleanProperty(SimpleString key, boolean value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putBooleanProperty(String key, boolean value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putByteProperty(SimpleString key, byte value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putByteProperty(String key, byte value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putBytesProperty(SimpleString key, byte[] value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putBytesProperty(String key, byte[] value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putDoubleProperty(SimpleString key, double value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putDoubleProperty(String key, double value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putFloatProperty(SimpleString key, float value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putFloatProperty(String key, float value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putIntProperty(SimpleString key, int value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putIntProperty(String key, int value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putLongProperty(SimpleString key, long value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putLongProperty(String key, long value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putObjectProperty(SimpleString key, Object value) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         
      }

      public void putObjectProperty(String key, Object value) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         
      }

      public void putShortProperty(SimpleString key, short value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putShortProperty(String key, short value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putStringProperty(SimpleString key, SimpleString value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putStringProperty(String key, String value)
      {
         // TODO Auto-generated method stub
         
      }

      public void putTypedProperties(TypedProperties properties)
      {
         // TODO Auto-generated method stub
         
      }

      public Object removeProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public Object removeProperty(String key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void resetCopied()
      {
         // TODO Auto-generated method stub
         
      }

      public void setDestination(SimpleString destination)
      {
         // TODO Auto-generated method stub
         
      }

      public void setDurable(boolean durable)
      {
         // TODO Auto-generated method stub
         
      }

      public void setExpiration(long expiration)
      {
         // TODO Auto-generated method stub
         
      }

      public void setPriority(byte priority)
      {
         // TODO Auto-generated method stub
         
      }

      public void setTimestamp(long timestamp)
      {
         // TODO Auto-generated method stub
         
      }

      public Map<String, Object> toMap()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void decode(HornetQBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      public void encode(HornetQBuffer buffer)
      {
         // TODO Auto-generated method stub
         
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
       * @see org.hornetq.core.postoffice.Binding#route(org.hornetq.core.server.ServerMessage, org.hornetq.core.server.RoutingContext)
       */
      public void route(ServerMessage message, RoutingContext context) throws Exception
      {
         // TODO Auto-generated method stub

      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
