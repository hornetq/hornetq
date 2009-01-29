/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.MapMessage;

import org.jboss.messaging.core.logging.Logger;

/**
 * A wrapper for a message
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: 71554 $
 */
public class JBMMapMessage extends JBMMessage implements MapMessage
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMapMessage.class);
   
   /** Whether trace is enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Create a new wrapper
    * 
    * @param message the message
    * @param session the session
    */
   public JBMMapMessage(MapMessage message, JBMSession session)
   {
      super(message, session);

      if (trace)
         log.trace("constructor(" + message + ", " + session + ")");
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public boolean getBoolean(String name) throws JMSException
   {
      if (trace)
         log.trace("getBoolean(" + name + ")");

      return ((MapMessage) message).getBoolean(name);
   }
   
   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public byte getByte(String name) throws JMSException
   {
      if (trace)
         log.trace("getByte(" + name + ")");

      return ((MapMessage) message).getByte(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public byte[] getBytes(String name) throws JMSException
   {
      if (trace)
         log.trace("getBytes(" + name + ")");

      return ((MapMessage) message).getBytes(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public char getChar(String name) throws JMSException
   {
      if (trace)
         log.trace("getChar(" + name + ")");

      return ((MapMessage) message).getChar(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public double getDouble(String name) throws JMSException
   {
      if (trace)
         log.trace("getDouble(" + name + ")");

      return ((MapMessage) message).getDouble(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public float getFloat(String name) throws JMSException
   {
      if (trace)
         log.trace("getFloat(" + name + ")");

      return ((MapMessage) message).getFloat(name);
   }
   
   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public int getInt(String name) throws JMSException
   {
      if (trace)
         log.trace("getInt(" + name + ")");

      return ((MapMessage) message).getInt(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public long getLong(String name) throws JMSException
   {
      if (trace)
         log.trace("getLong(" + name + ")");

      return ((MapMessage) message).getLong(name);
   }

   /**
    * Get the map names
    * @return The values 
    * @exception JMSException Thrown if an error occurs
    */
   public Enumeration getMapNames() throws JMSException
   {
      if (trace)
         log.trace("getMapNames()");

      return ((MapMessage) message).getMapNames();
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public Object getObject(String name) throws JMSException
   {
      if (trace)
         log.trace("getObject(" + name + ")");

      return ((MapMessage) message).getObject(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public short getShort(String name) throws JMSException
   {
      if (trace)
         log.trace("getShort(" + name + ")");

      return ((MapMessage) message).getShort(name);
   }

   /**
    * Get
    * @param name The name
    * @return The value 
    * @exception JMSException Thrown if an error occurs
    */
   public String getString(String name) throws JMSException
   {
      if (trace)
         log.trace("getString(" + name + ")");

      return ((MapMessage) message).getString(name);
   }

   /**
    * Does the item exist
    * @param name The name
    * @return True / false
    * @exception JMSException Thrown if an error occurs
    */
   public boolean itemExists(String name) throws JMSException
   {
      if (trace)
         log.trace("itemExists(" + name + ")");

      return ((MapMessage) message).itemExists(name);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setBoolean(String name, boolean value) throws JMSException
   {
      if (trace)
         log.trace("setBoolean(" + name + ", " + value + ")");

      ((MapMessage) message).setBoolean(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setByte(String name, byte value) throws JMSException
   {
      if (trace)
         log.trace("setByte(" + name + ", " + value + ")");

      ((MapMessage) message).setByte(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @param offset The offset
    * @param length The length
    * @exception JMSException Thrown if an error occurs
    */
   public void setBytes(String name, byte[] value, int offset, int length) throws JMSException
   {
      if (trace)
         log.trace("setBytes(" + name + ", " + value + ", " + offset + ", " + length + ")");

      ((MapMessage) message).setBytes(name, value, offset, length);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setBytes(String name, byte[] value) throws JMSException
   {
      if (trace)
         log.trace("setBytes(" + name + ", " + value + ")");

      ((MapMessage) message).setBytes(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setChar(String name, char value) throws JMSException
   {
      if (trace)
         log.trace("setChar(" + name + ", " + value + ")");

      ((MapMessage) message).setChar(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setDouble(String name, double value) throws JMSException
   {
      if (trace)
         log.trace("setDouble(" + name + ", " + value + ")");

      ((MapMessage) message).setDouble(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setFloat(String name, float value) throws JMSException
   {
      if (trace)
         log.trace("setFloat(" + name + ", " + value + ")");

      ((MapMessage) message).setFloat(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setInt(String name, int value) throws JMSException
   {
      if (trace)
         log.trace("setInt(" + name + ", " + value + ")");

      ((MapMessage) message).setInt(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setLong(String name, long value) throws JMSException
   {
      if (trace)
         log.trace("setLong(" + name + ", " + value + ")");

      ((MapMessage) message).setLong(name, value);
   }
   
   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setObject(String name, Object value) throws JMSException
   {
      if (trace)
         log.trace("setObject(" + name + ", " + value + ")");

      ((MapMessage) message).setObject(name, value);
   }
   
   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setShort(String name, short value) throws JMSException
   {
      if (trace)
         log.trace("setShort(" + name + ", " + value + ")");

      ((MapMessage) message).setShort(name, value);
   }

   /**
    * Set
    * @param name The name
    * @param value The value 
    * @exception JMSException Thrown if an error occurs
    */
   public void setString(String name, String value) throws JMSException
   {
      if (trace)
         log.trace("setString(" + name + ", " + value + ")");

      ((MapMessage) message).setString(name, value);
   }
}
