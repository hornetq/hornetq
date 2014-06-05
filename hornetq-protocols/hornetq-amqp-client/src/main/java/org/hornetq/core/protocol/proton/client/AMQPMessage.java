/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.core.protocol.proton.client;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import org.apache.qpid.proton.engine.Delivery;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQPropertyConversionException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.client.impl.ClientMessageInternal;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

/**
 * @author Clebert Suconic
 */

public class AMQPMessage implements ClientMessageInternal
{

   private final Delivery delivery;

   public AMQPMessage(Delivery delivery)
   {
      this.delivery = delivery;
   }

   @Override
   public long getMessageID()
   {
      return 0;
   }

   @Override
   public UUID getUserID()
   {
      return null;
   }

   @Override
   public void setUserID(UUID userID)
   {

   }

   @Override
   public SimpleString getAddress()
   {
      return null;
   }

   @Override
   public Message setAddress(SimpleString address)
   {
      return null;
   }

   @Override
   public byte getType()
   {
      return 0;
   }

   @Override
   public boolean isDurable()
   {
      return false;
   }

   @Override
   public void setDurable(boolean durable)
   {

   }

   @Override
   public long getExpiration()
   {
      return 0;
   }

   @Override
   public boolean isExpired()
   {
      return false;
   }

   @Override
   public void setExpiration(long expiration)
   {

   }

   @Override
   public long getTimestamp()
   {
      return 0;
   }

   @Override
   public void setTimestamp(long timestamp)
   {

   }

   @Override
   public byte getPriority()
   {
      return 0;
   }

   @Override
   public void setPriority(byte priority)
   {

   }

   @Override
   public int getEncodeSize()
   {
      return 0;
   }

   @Override
   public boolean isLargeMessage()
   {
      return false;
   }

   @Override
   public HornetQBuffer getBodyBuffer()
   {
      return null;
   }

   @Override
   public HornetQBuffer getBodyBufferCopy()
   {
      return null;
   }

   @Override
   public Message putBooleanProperty(SimpleString key, boolean value)
   {
      return null;
   }

   @Override
   public Message putBooleanProperty(String key, boolean value)
   {
      return null;
   }

   @Override
   public Message putByteProperty(SimpleString key, byte value)
   {
      return null;
   }

   @Override
   public Message putByteProperty(String key, byte value)
   {
      return null;
   }

   @Override
   public Message putBytesProperty(SimpleString key, byte[] value)
   {
      return null;
   }

   @Override
   public Message putBytesProperty(String key, byte[] value)
   {
      return null;
   }

   @Override
   public Message putShortProperty(SimpleString key, short value)
   {
      return null;
   }

   @Override
   public Message putShortProperty(String key, short value)
   {
      return null;
   }

   @Override
   public Message putCharProperty(SimpleString key, char value)
   {
      return null;
   }

   @Override
   public Message putCharProperty(String key, char value)
   {
      return null;
   }

   @Override
   public Message putIntProperty(SimpleString key, int value)
   {
      return null;
   }

   @Override
   public Message putIntProperty(String key, int value)
   {
      return null;
   }

   @Override
   public Message putLongProperty(SimpleString key, long value)
   {
      return null;
   }

   @Override
   public Message putLongProperty(String key, long value)
   {
      return null;
   }

   @Override
   public Message putFloatProperty(SimpleString key, float value)
   {
      return null;
   }

   @Override
   public Message putFloatProperty(String key, float value)
   {
      return null;
   }

   @Override
   public Message putDoubleProperty(SimpleString key, double value)
   {
      return null;
   }

   @Override
   public Message putDoubleProperty(String key, double value)
   {
      return null;
   }

   @Override
   public Message putStringProperty(SimpleString key, SimpleString value)
   {
      return null;
   }

   @Override
   public Message putStringProperty(String key, String value)
   {
      return null;
   }

   @Override
   public Message putObjectProperty(SimpleString key, Object value) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Message putObjectProperty(String key, Object value) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Object removeProperty(SimpleString key)
   {
      return null;
   }

   @Override
   public Object removeProperty(String key)
   {
      return null;
   }

   @Override
   public boolean containsProperty(SimpleString key)
   {
      return false;
   }

   @Override
   public boolean containsProperty(String key)
   {
      return false;
   }

   @Override
   public Boolean getBooleanProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return false;
   }

   @Override
   public Boolean getBooleanProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Byte getByteProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Byte getByteProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Double getDoubleProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Double getDoubleProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Integer getIntProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Integer getIntProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Long getLongProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Long getLongProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Object getObjectProperty(SimpleString key)
   {
      return null;
   }

   @Override
   public Object getObjectProperty(String key)
   {
      return null;
   }

   @Override
   public Short getShortProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Short getShortProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Float getFloatProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public Float getFloatProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public String getStringProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public String getStringProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public SimpleString getSimpleStringProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public SimpleString getSimpleStringProperty(String key) throws HornetQPropertyConversionException
   {
      return null;
   }

   @Override
   public byte[] getBytesProperty(SimpleString key) throws HornetQPropertyConversionException
   {
      return new byte[0];
   }

   @Override
   public byte[] getBytesProperty(String key) throws HornetQPropertyConversionException
   {
      return new byte[0];
   }

   @Override
   public Set<SimpleString> getPropertyNames()
   {
      return null;
   }

   @Override
   public Map<String, Object> toMap()
   {
      return null;
   }

   @Override
   public TypedProperties getProperties()
   {
      return null;
   }

   @Override
   public void setAddressTransient(SimpleString address)
   {

   }

   @Override
   public void onReceipt(ClientConsumerInternal consumer)
   {

   }

   @Override
   public void discardBody()
   {

   }

   @Override
   public boolean isCompressed()
   {
      return false;
   }

   @Override
   public int getFlowControlSize()
   {
      return 1;
   }

   @Override
   public void setFlowControlSize(int flowControlSize)
   {

   }

   @Override
   public int getDeliveryCount()
   {
      return 0;
   }

   @Override
   public void setDeliveryCount(int deliveryCount)
   {

   }

   @Override
   public void acknowledge() throws HornetQException
   {

   }

   @Override
   public void individualAcknowledge() throws HornetQException
   {

   }

   @Override
   public void checkCompletion() throws HornetQException
   {

   }

   @Override
   public int getBodySize()
   {
      return 0;
   }

   @Override
   public void setOutputStream(OutputStream out) throws HornetQException
   {

   }

   @Override
   public void saveToOutputStream(OutputStream out) throws HornetQException
   {

   }

   @Override
   public boolean waitOutputStreamCompletion(long timeMilliseconds) throws HornetQException
   {
      return false;
   }

   @Override
   public void setBodyInputStream(InputStream bodyInputStream)
   {

   }
}
