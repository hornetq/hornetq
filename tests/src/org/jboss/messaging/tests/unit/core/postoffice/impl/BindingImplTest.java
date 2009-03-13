/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.unit.core.postoffice.impl;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.BindingType;
import org.jboss.messaging.core.postoffice.impl.BindingsImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.TypedProperties;

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
      for (int i = 0; i < 2000; i++)
      {
         internalTestRoute();
      }
   }
   
   private void internalTestRoute() throws Exception
   {
      
      final CountDownLatch latchAlign = new CountDownLatch(1);
      final CountDownLatch latchStart = new CountDownLatch(1);

      final FakeBinding fake = new FakeBinding(new SimpleString("a"));
      
      final BindingsImpl bind = new BindingsImpl();
      bind.addBinding(fake);
      bind.addBinding(new FakeBinding(new SimpleString("a")));
      bind.addBinding(new FakeBinding(new SimpleString("a")));
      
      
      
      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               latchAlign.countDown();
               latchStart.await();
               bind.removeBinding(fake);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };
      
      t.start();
      
      latchAlign.await();
      latchStart.countDown();     
      
      bind.route(new FakeMessage(), new FakeTransaction());
   }
   
   public void testRemoveWhileRedistributing() throws Exception
   {
      // It would require many iterations before getting a failure
      for (int i = 0; i < 2000; i++)
      {
         internalTestRedistribute();
      }
   }
   
   
   private void internalTestRedistribute() throws Exception
   {
      
      final CountDownLatch latchAlign = new CountDownLatch(1);
      final CountDownLatch latchStart = new CountDownLatch(1);

      final FakeBinding fake = new FakeBinding(new SimpleString("a"));
      
      final BindingsImpl bind = new BindingsImpl();
      bind.addBinding(fake);
      bind.addBinding(new FakeBinding(new SimpleString("a")));
      bind.addBinding(new FakeBinding(new SimpleString("a")));
      
      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               latchAlign.countDown();
               latchStart.await();
               bind.removeBinding(fake);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      };
      
      t.start();
      
      latchAlign.await();
      latchStart.countDown();     

      bind.redistribute(new FakeMessage(), new SimpleString("a"), new FakeTransaction());
   }
   
   
   class FakeTransaction implements Transaction
   {

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#addOperation(org.jboss.messaging.core.transaction.TransactionOperation)
       */
      public void addOperation(TransactionOperation sync)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#commit()
       */
      public void commit() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#commit(boolean)
       */
      public void commit(boolean onePhase) throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#getCreateTime()
       */
      public long getCreateTime()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#getID()
       */
      public long getID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#getOperationsCount()
       */
      public int getOperationsCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#getProperty(int)
       */
      public Object getProperty(int index)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#getState()
       */
      public State getState()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#getXid()
       */
      public Xid getXid()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#markAsRollbackOnly(org.jboss.messaging.core.exception.MessagingException)
       */
      public void markAsRollbackOnly(MessagingException messagingException)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#prepare()
       */
      public void prepare() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#putProperty(int, java.lang.Object)
       */
      public void putProperty(int index, Object property)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#removeOperation(org.jboss.messaging.core.transaction.TransactionOperation)
       */
      public void removeOperation(TransactionOperation sync)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#resume()
       */
      public void resume()
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#rollback()
       */
      public void rollback() throws Exception
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#setState(org.jboss.messaging.core.transaction.Transaction.State)
       */
      public void setState(State state)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.transaction.Transaction#suspend()
       */
      public void suspend()
      {
         // TODO Auto-generated method stub
         
      }
      
   }
   
   
   class FakeMessage implements ServerMessage
   {

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#copy(long)
       */
      public ServerMessage copy(long newID) throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#copy()
       */
      public ServerMessage copy() throws Exception
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#createReference(org.jboss.messaging.core.server.Queue)
       */
      public MessageReference createReference(Queue queue)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#decrementDurableRefCount()
       */
      public int decrementDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#decrementRefCount()
       */
      public int decrementRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#getMemoryEstimate()
       */
      public int getMemoryEstimate()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#getRefCount()
       */
      public int getRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#incrementDurableRefCount()
       */
      public int incrementDurableRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#incrementRefCount()
       */
      public int incrementRefCount()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#isLargeMessage()
       */
      public boolean isLargeMessage()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#isStored()
       */
      public boolean isStored()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#setMessageID(long)
       */
      public void setMessageID(long id)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.server.ServerMessage#setStored()
       */
      public void setStored()
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#containsProperty(org.jboss.messaging.utils.SimpleString)
       */
      public boolean containsProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#decode(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void decode(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#decodeBody(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void decodeBody(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#decodeProperties(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void decodeProperties(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#encode(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void encode(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#encodeBody(org.jboss.messaging.core.remoting.spi.MessagingBuffer, long, int)
       */
      public void encodeBody(MessagingBuffer buffer, long start, int size)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#encodeBody(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void encodeBody(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#encodeProperties(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void encodeProperties(MessagingBuffer buffer)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getBody()
       */
      public MessagingBuffer getBody()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getBodySize()
       */
      public int getBodySize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getDestination()
       */
      public SimpleString getDestination()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getEncodeSize()
       */
      public int getEncodeSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getExpiration()
       */
      public long getExpiration()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getMessageID()
       */
      public long getMessageID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getPriority()
       */
      public byte getPriority()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getProperties()
       */
      public TypedProperties getProperties()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getPropertiesEncodeSize()
       */
      public int getPropertiesEncodeSize()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getProperty(org.jboss.messaging.utils.SimpleString)
       */
      public Object getProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getPropertyNames()
       */
      public Set<SimpleString> getPropertyNames()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getTimestamp()
       */
      public long getTimestamp()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#getType()
       */
      public byte getType()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#isDurable()
       */
      public boolean isDurable()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#isExpired()
       */
      public boolean isExpired()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putBooleanProperty(org.jboss.messaging.utils.SimpleString, boolean)
       */
      public void putBooleanProperty(SimpleString key, boolean value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putByteProperty(org.jboss.messaging.utils.SimpleString, byte)
       */
      public void putByteProperty(SimpleString key, byte value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putBytesProperty(org.jboss.messaging.utils.SimpleString, byte[])
       */
      public void putBytesProperty(SimpleString key, byte[] value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putDoubleProperty(org.jboss.messaging.utils.SimpleString, double)
       */
      public void putDoubleProperty(SimpleString key, double value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putFloatProperty(org.jboss.messaging.utils.SimpleString, float)
       */
      public void putFloatProperty(SimpleString key, float value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putIntProperty(org.jboss.messaging.utils.SimpleString, int)
       */
      public void putIntProperty(SimpleString key, int value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putLongProperty(org.jboss.messaging.utils.SimpleString, long)
       */
      public void putLongProperty(SimpleString key, long value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putShortProperty(org.jboss.messaging.utils.SimpleString, short)
       */
      public void putShortProperty(SimpleString key, short value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putStringProperty(org.jboss.messaging.utils.SimpleString, org.jboss.messaging.utils.SimpleString)
       */
      public void putStringProperty(SimpleString key, SimpleString value)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#putTypedProperties(org.jboss.messaging.utils.TypedProperties)
       */
      public void putTypedProperties(TypedProperties properties)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#removeProperty(org.jboss.messaging.utils.SimpleString)
       */
      public Object removeProperty(SimpleString key)
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#setBody(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void setBody(MessagingBuffer body)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#setDestination(org.jboss.messaging.utils.SimpleString)
       */
      public void setDestination(SimpleString destination)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#setDurable(boolean)
       */
      public void setDurable(boolean durable)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#setExpiration(long)
       */
      public void setExpiration(long expiration)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#setPriority(byte)
       */
      public void setPriority(byte priority)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.message.Message#setTimestamp(long)
       */
      public void setTimestamp(long timestamp)
      {
         // TODO Auto-generated method stub
         
      }
      
   }

   
   
   class FakeFilter implements Filter
   {

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.filter.Filter#getFilterString()
       */
      public SimpleString getFilterString()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.filter.Filter#match(org.jboss.messaging.core.server.ServerMessage)
       */
      public boolean match(ServerMessage message)
      {
         return false;
      }
      
   }
   
   
   class FakeBinding implements Binding
   {

      final SimpleString name;
      
      FakeBinding(SimpleString name)
      {
         this.name = name;
      }
      
      public SimpleString getAddress()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getBindable()
       */
      public Bindable getBindable()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getClusterName()
       */
      public SimpleString getClusterName()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getDistance()
       */
      public int getDistance()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getFilter()
       */
      public Filter getFilter()
      {
         return new FakeFilter();
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getID()
       */
      public int getID()
      {
         // TODO Auto-generated method stub
         return 0;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getRoutingName()
       */
      public SimpleString getRoutingName()
      {
         return this.name;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getType()
       */
      public BindingType getType()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#getUniqueName()
       */
      public SimpleString getUniqueName()
      {
         // TODO Auto-generated method stub
         return null;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#isExclusive()
       */
      public boolean isExclusive()
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#isHighAcceptPriority(org.jboss.messaging.core.server.ServerMessage)
       */
      public boolean isHighAcceptPriority(ServerMessage message)
      {
         // TODO Auto-generated method stub
         return false;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#setID(int)
       */
      public void setID(int id)
      {
         // TODO Auto-generated method stub
         
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.postoffice.Binding#willRoute(org.jboss.messaging.core.server.ServerMessage)
       */
      public void willRoute(ServerMessage message)
      {
         // TODO Auto-generated method stub
         
      }
      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
