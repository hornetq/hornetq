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
package org.jboss.jms.message;

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageNotWriteableException;

/**
 * 
 * Thin proxy for a JBossMapMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * MapMessageProxy.java,v 1.1 2006/03/08 08:00:34 timfox Exp
 */
public class MapMessageProxy extends MessageProxy implements MapMessage
{
   private static final long serialVersionUID = 6953530870351885569L;

   public MapMessageProxy(long deliveryId, JBossMapMessage message, int deliveryCount)
   {
      super(deliveryId, message, deliveryCount);
   }
   
   public MapMessageProxy(JBossMapMessage message)
   {
      super(message);
   }
   
   public boolean getBoolean(String name) throws JMSException
   {
      return ((MapMessage)message).getBoolean(name);
   }

   public byte getByte(String name) throws JMSException
   {
      return ((MapMessage)message).getByte(name);
   }

   public short getShort(String name) throws JMSException
   {
      return ((MapMessage)message).getShort(name);
   }

   public char getChar(String name) throws JMSException
   {
      return ((MapMessage)message).getChar(name);
   }

   public int getInt(String name) throws JMSException
   {
      return ((MapMessage)message).getInt(name);
   }

   public long getLong(String name) throws JMSException
   {
      return ((MapMessage)message).getLong(name);
   }

   public float getFloat(String name) throws JMSException
   {
      return ((MapMessage)message).getFloat(name);
   }

   public double getDouble(String name) throws JMSException
   {
      return ((MapMessage)message).getDouble(name);
   }

   public String getString(String name) throws JMSException
   {
      return ((MapMessage)message).getString(name);
   }

   public byte[] getBytes(String name) throws JMSException
   {
      return ((MapMessage)message).getBytes(name);
   }

   public Object getObject(String name) throws JMSException
   {
      return ((MapMessage)message).getObject(name);
   }

   public Enumeration getMapNames() throws JMSException
   {
      return ((MapMessage)message).getMapNames();
   }

   public void setBoolean(String name, boolean value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setBoolean(name, value);
   }

   public void setByte(String name, byte value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setByte(name, value);
   }

   public void setShort(String name, short value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setShort(name, value);
   }

   public void setChar(String name, char value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setChar(name, value);
   }

   public void setInt(String name, int value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setInt(name, value);
   }

   public void setLong(String name, long value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setLong(name, value);
   }

   public void setFloat(String name, float value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setFloat(name, value);
   }

   public void setDouble(String name, double value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setDouble(name, value);
   }

   public void setString(String name, String value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setString(name, value);
   }

   public void setBytes(String name, byte[] value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setBytes(name, value);
   }

   public void setBytes(String name, byte[] value, int offset, int length) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setBytes(name, value, offset, length);
   }

   public void setObject(String name, Object value) throws JMSException
   {
      if (bodyReadOnly)
      {
         throw new MessageNotWriteableException("Message is ReadOnly !");
      }
      bodyChange();
      ((MapMessage)message).setObject(name, value);
   }

   public boolean itemExists(String name) throws JMSException
   {
      return ((MapMessage)message).itemExists(name);
   }
}
