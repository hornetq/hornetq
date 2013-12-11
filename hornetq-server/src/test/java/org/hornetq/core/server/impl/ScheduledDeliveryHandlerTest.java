/*
 * Copyright 2013 Red Hat, Inc.
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


import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.message.BodyEncoder;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;
import org.junit.*;

/**
 * @author Clebert Suconic
 */

public class ScheduledDeliveryHandlerTest extends Assert
{

   @Test
   public void testScheduleRandom() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);


      long nextMessage = 0;
      long NUMBER_OF_SEQUENCES = 100000;

      for (int i = 0; i < NUMBER_OF_SEQUENCES; i++)
      {
         int numberOfMessages = RandomUtil.randomInt() % 10;
         if (numberOfMessages == 0) numberOfMessages = 1;

         long nextScheduledTime = RandomUtil.randomPositiveLong();

         for (int j = 0; j < numberOfMessages; j++)
         {
            boolean tail = RandomUtil.randomBoolean();

            //System.out.println("addMessage(handler, " + nextMessage + ", " + nextScheduledTime + "," + tail + ");");

            addMessage(handler, nextMessage++, nextScheduledTime, tail);
         }
      }

      debugList(true, handler, nextMessage);

   }

   @Test
   public void testScheduleSameTimeHeadAndTail() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);

      long time = System.currentTimeMillis() + 10000;
      for (int i = 10001 ; i < 20000; i++)
      {
         addMessage(handler, i, time, true);
      }
      addMessage(handler, 10000, time, false);


      time = System.currentTimeMillis() + 5000;
      for (int i = 1 ; i < 10000; i++)
      {
         addMessage(handler, i, time, true);
      }
      addMessage(handler, 0, time, false);

      debugList(true, handler, 20000);

      validateSequence(handler);

   }

   @Test
   public void testScheduleFixedSample() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);


      addMessage(handler, 0, 48l, true);
      addMessage(handler, 1, 75l, true);
      addMessage(handler, 2, 56l, true);
      addMessage(handler, 3, 7l, false);
      addMessage(handler, 4, 69l, true);

      debugList(true, handler, 5);

   }


   @Test
   public void testScheduleFixedSampleTailAndHead() throws Exception
   {
      ScheduledDeliveryHandlerImpl handler = new ScheduledDeliveryHandlerImpl(null);


      // mix a sequence of tails / heads, but at the end this was supposed to be all sequential
      addMessage(handler, 1, 48l, true);
      addMessage(handler, 2, 48l, true);
      addMessage(handler, 3, 48l, true);
      addMessage(handler, 4, 48l, true);
      addMessage(handler, 5, 48l, true);
      addMessage(handler, 0, 48l, false);


      addMessage(handler, 13, 59l, true);
      addMessage(handler, 14, 59l, true);
      addMessage(handler, 15, 59l, true);
      addMessage(handler, 16, 59l, true);
      addMessage(handler, 17, 59l, true);
      addMessage(handler, 12, 59l, false);


      addMessage(handler, 7, 49l, true);
      addMessage(handler, 8, 49l, true);
      addMessage(handler, 9, 49l, true);
      addMessage(handler, 10, 49l, true);
      addMessage(handler, 11, 49l, true);
      addMessage(handler, 6, 49l, false);

      validateSequence(handler);
   }

   private void validateSequence(ScheduledDeliveryHandlerImpl handler)
   {
      long lastSequence = -1;
      for (MessageReference ref : handler.getScheduledReferences())
      {
         assertEquals(lastSequence + 1, ref.getMessage().getMessageID());
         lastSequence = ref.getMessage().getMessageID();
      }
   }

   private void addMessage(ScheduledDeliveryHandlerImpl handler, long nextMessageID, long nextScheduledTime, boolean tail)
   {
      MessageReferenceImpl refImpl = new MessageReferenceImpl(new FakeMessage(nextMessageID), null);
      refImpl.setScheduledDeliveryTime(nextScheduledTime);
      handler.addInPlace(nextScheduledTime, refImpl, tail);
   }


   private void debugList(boolean fail, ScheduledDeliveryHandlerImpl handler, long numberOfExpectedMessages)
   {
      List<MessageReference> refs = handler.getScheduledReferences();

      HashSet<Long> messages = new HashSet<Long>();

      long lastTime = -1;

      //System.out.println(":::::::::::::::::::::::::::");
      for (MessageReference ref : refs)
      {
        // System.out.println("Time: " + ref.getScheduledDeliveryTime());
         assertFalse(messages.contains(ref.getMessage().getMessageID()));
         messages.add(ref.getMessage().getMessageID());

         if (fail)
         {
            assertTrue(ref.getScheduledDeliveryTime() >= lastTime);
         }
         else
         {
            if (ref.getScheduledDeliveryTime() < lastTime)
            {
               System.out.println("^^^fail at " + ref.getScheduledDeliveryTime());
            }
         }
         lastTime = ref.getScheduledDeliveryTime();
      }

      for (long i = 0 ; i < numberOfExpectedMessages; i++)
      {
         assertTrue(messages.contains(Long.valueOf(i)));
      }
      System.out.println("");
   }

   class FakeMessage implements ServerMessage
   {

      final long id;

      public FakeMessage(final long id)
      {
         this.id = id;
      }

      @Override
      public void setMessageID(long id)
      {
      }

      @Override
      public long getMessageID()
      {
         return id;
      }


      @Override
      public MessageReference createReference(Queue queue)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void forceAddress(SimpleString address)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int incrementRefCount() throws Exception
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int decrementRefCount() throws Exception
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int incrementDurableRefCount()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int decrementDurableRefCount()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public ServerMessage copy(long newID)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void finishCopy() throws Exception
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public ServerMessage copy()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int getMemoryEstimate()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int getRefCount()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public ServerMessage makeCopyForExpiryOrDLA(long newID, MessageReference originalReference, boolean expiry) throws Exception
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setOriginalHeaders(ServerMessage other, MessageReference originalReference, boolean expiry)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setPagingStore(PagingStore store)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public PagingStore getPagingStore()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean hasInternalProperties()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean storeIsPaging()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void encodeMessageIDToBuffer()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public byte[] getDuplicateIDBytes()
      {
         return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Object getDuplicateProperty()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void encode(HornetQBuffer buffer)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void decode(HornetQBuffer buffer)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void decodeFromBuffer(HornetQBuffer buffer)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int getEndOfMessagePosition()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int getEndOfBodyPosition()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void checkCopy()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void bodyChanged()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void resetCopied()
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean isServerMessage()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public HornetQBuffer getEncodedBuffer()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int getHeadersAndPropertiesEncodeSize()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public HornetQBuffer getWholeBuffer()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void encodeHeadersAndProperties(HornetQBuffer buffer)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void decodeHeadersAndProperties(HornetQBuffer buffer)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public BodyEncoder getBodyEncoder() throws HornetQException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public InputStream getBodyInputStream()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setAddressTransient(SimpleString address)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public TypedProperties getTypedProperties()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public UUID getUserID()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setUserID(UUID userID)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public SimpleString getAddress()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message setAddress(SimpleString address)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public byte getType()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean isDurable()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setDurable(boolean durable)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public long getExpiration()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean isExpired()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setExpiration(long expiration)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public long getTimestamp()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setTimestamp(long timestamp)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public byte getPriority()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public void setPriority(byte priority)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public int getEncodeSize()
      {
         return 0;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean isLargeMessage()
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public HornetQBuffer getBodyBuffer()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public HornetQBuffer getBodyBufferCopy()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putBooleanProperty(SimpleString key, boolean value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putBooleanProperty(String key, boolean value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putByteProperty(SimpleString key, byte value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putByteProperty(String key, byte value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putBytesProperty(SimpleString key, byte[] value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putBytesProperty(String key, byte[] value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putShortProperty(SimpleString key, short value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putShortProperty(String key, short value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putCharProperty(SimpleString key, char value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putCharProperty(String key, char value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putIntProperty(SimpleString key, int value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putIntProperty(String key, int value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putLongProperty(SimpleString key, long value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putLongProperty(String key, long value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putFloatProperty(SimpleString key, float value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putFloatProperty(String key, float value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putDoubleProperty(SimpleString key, double value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putDoubleProperty(String key, double value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putStringProperty(SimpleString key, SimpleString value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putStringProperty(String key, String value)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putObjectProperty(SimpleString key, Object value) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Message putObjectProperty(String key, Object value) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Object removeProperty(SimpleString key)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Object removeProperty(String key)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean containsProperty(SimpleString key)
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public boolean containsProperty(String key)
      {
         return false;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Boolean getBooleanProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Boolean getBooleanProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Byte getByteProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Byte getByteProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Double getDoubleProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Double getDoubleProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Integer getIntProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Integer getIntProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Long getLongProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Long getLongProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Object getObjectProperty(SimpleString key)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Object getObjectProperty(String key)
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Short getShortProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Short getShortProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Float getFloatProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Float getFloatProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public String getStringProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public String getStringProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public SimpleString getSimpleStringProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public SimpleString getSimpleStringProperty(String key) throws HornetQPropertyConversionException
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public byte[] getBytesProperty(SimpleString key) throws HornetQPropertyConversionException
      {
         return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public byte[] getBytesProperty(String key) throws HornetQPropertyConversionException
      {
         return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Set<SimpleString> getPropertyNames()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }

      @Override
      public Map<String, Object> toMap()
      {
         return null;  //To change body of implemented methods use File | Settings | File Templates.
      }
   }

}
