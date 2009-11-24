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
import java.util.Map;
import java.util.Set;

import javax.transaction.xa.Xid;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.message.PropertyConversionException;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.impl.BindingsImpl;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.Bindable;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.RoutingContextImpl;
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
            bind.route(new FakeMessage(), new RoutingContextImpl(new FakeTransaction()));
         }
         else
         {
            bind.redistribute(new FakeMessage(), queue, new RoutingContextImpl(new FakeTransaction()));
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

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#makeCopyForExpiryOrDLA(long, boolean)
       */
      public ServerMessage makeCopyForExpiryOrDLA(long newID, boolean expiry) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#setOriginalHeaders(org.hornetq.core.server.ServerMessage, boolean)
       */
      public void setOriginalHeaders(ServerMessage other, boolean expiry)
      {
         // TODO Auto-generated method stub

      }

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
       * @see org.hornetq.core.message.Message#decode(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void decode(final HornetQBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#decodeBody(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void decodeBody(final HornetQBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#decodeProperties(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void decodeHeadersAndProperties(final HornetQBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encode(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void encode(final HornetQBuffer buffer)
      {

      }

      public void encodeBody(HornetQBuffer bufferOut, BodyEncoder context, int size)
      {
         // To change body of implemented methods use File | Settings | File Templates.
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encodeBody(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void encodeBody(final HornetQBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#encodeProperties(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void encodeHeadersAndProperties(final HornetQBuffer buffer)
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBody()
       */
      public HornetQBuffer getBody()
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
      public int getHeadersAndPropertiesEncodeSize()
      {

         return 0;
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
       * @see org.hornetq.core.message.Message#getSimpleStringProperty(org.hornetq.utils.SimpleString)
       */
      public SimpleString getSimpleStringProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getSimpleStringProperty(java.lang.String)
       */
      public SimpleString getSimpleStringProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      public void putObjectProperty(SimpleString key, Object value)
      {
         // TODO Auto-generated method stub
         
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#putObjectProperty(java.lang.String, java.lang.Object)
       */
      public void putObjectProperty(String key, Object value)
      {
         // TODO Auto-generated method stub
         
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
       * @see org.hornetq.core.message.Message#setBody(org.hornetq.core.remoting.spi.HornetQBuffer)
       */
      public void setBody(final HornetQBuffer body)
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
      public Object getObjectProperty(String key)
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

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getStringProperty(org.hornetq.utils.SimpleString)
       */
      public String getStringProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBooleanProperty(org.hornetq.utils.SimpleString)
       */
      public Boolean getBooleanProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getByteProperty(org.hornetq.utils.SimpleString)
       */
      public Byte getByteProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBytesProperty(org.hornetq.utils.SimpleString)
       */
      public byte[] getBytesProperty(SimpleString key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getDoubleProperty(org.hornetq.utils.SimpleString)
       */
      public Double getDoubleProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return 0.0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getFloatProperty(org.hornetq.utils.SimpleString)
       */
      public Float getFloatProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return 0f;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getIntProperty(org.hornetq.utils.SimpleString)
       */
      public Integer getIntProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getLongProperty(org.hornetq.utils.SimpleString)
       */
      public Long getLongProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return 0L;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getObjectProperty(org.hornetq.utils.SimpleString)
       */
      public Object getObjectProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getShortProperty(org.hornetq.utils.SimpleString)
       */
      public Short getShortProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return 0;
      }
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBooleanProperty(java.lang.String)
       */
      public Boolean getBooleanProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getByteProperty(java.lang.String)
       */
      public Byte getByteProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBytesProperty(java.lang.String)
       */
      public byte[] getBytesProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getDoubleProperty(java.lang.String)
       */
      public Double getDoubleProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getFloatProperty(java.lang.String)
       */
      public Float getFloatProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getIntProperty(java.lang.String)
       */
      public Integer getIntProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getLongProperty(java.lang.String)
       */
      public Long getLongProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getShortProperty(java.lang.String)
       */
      public Short getShortProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }
      
      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getStringProperty(java.lang.String)
       */
      public String getStringProperty(String key) throws PropertyConversionException
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#decrementRefCount(org.hornetq.core.paging.PagingStore, org.hornetq.core.server.MessageReference)
       */
      public int decrementRefCount(PagingStore pagingStore, MessageReference reference) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.ServerMessage#incrementRefCount(org.hornetq.core.paging.PagingStore, org.hornetq.core.server.MessageReference)
       */
      public int incrementRefCount(PagingStore pagingStore, MessageReference reference) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public PagingStore getPagingStore()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public void setPagingStore(PagingStore store)
      {
         // TODO Auto-generated method stub
         
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

      public boolean storeIsPaging()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.message.Message#getBodyEncoder()
       */
      public BodyEncoder getBodyEncoder()
      {
         // TODO Auto-generated method stub
         return null;
      }

      public int decrementRefCount(MessageReference reference)
      {
         // TODO Auto-generated method stub
         return 0;
      }

      public int incrementRefCount(MessageReference reference) throws Exception
      {
         // TODO Auto-generated method stub
         return 0;
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
