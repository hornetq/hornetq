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
package org.jboss.messaging.core.message;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.util.SafeUTF;
import org.jboss.util.Primitives;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public abstract class RoutableSupport implements Routable, Externalizable
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RoutableSupport.class);
   
   /** A byte property */
   protected static final byte BYTE = 0;
   /** A short property */
   protected static final byte SHORT = 1;
   /** An integer property */
   protected static final byte INT = 2;
   /** A long property */
   protected static final byte LONG = 3;
   /** A float property */
   protected static final byte FLOAT = 4;
   /** A double property */
   protected static final byte DOUBLE = 5;
   /** A boolean property */
   protected static final byte BOOLEAN = 6;
   /** A string property */
   protected static final byte STRING = 7;
   /** An object property */
   protected static final byte OBJECT = 8;
   /** A null property */
   protected static final byte NULL = 9;
   
   protected static final byte BYTES = 10;
   
   protected static final byte MAP = 11;
   
   protected static final byte LIST = 12;

   // Static --------------------------------------------------------
   
   public static void writeList(ObjectOutput out, List list) throws IOException
   {
      out.writeInt(list.size());
      Iterator iter = list.iterator();
      while (iter.hasNext())
      {
         Object value = iter.next();
         if (value != null && !(value instanceof Serializable))
         {
            throw new IOException("Object in List must be serializable: " + value);
         }
         internalWriteObject(out, (Serializable)value, false, false);
      }
   }
   
   public static List readList(ObjectInput in) throws ClassNotFoundException, IOException
   {
      int size = in.readInt();
      ArrayList list = new ArrayList(size);
      for (int i = 0; i < size; i++)
      {
         Object obj = internalReadObject(in, false);
         list.add(obj);
      }
      return list;
   }
   
   protected static Serializable internalReadObject(ObjectInput in, boolean longStrings) throws IOException, ClassNotFoundException
   {
      byte type = in.readByte();
      Serializable value = null;
      switch (type)
      {
         case BYTE :
            value = new Byte(in.readByte());
            break;
         case SHORT :
            value = new Short(in.readShort());
            break;
         case INT :
            value = new Integer(in.readInt());
            break;
         case LONG :
            value = new Long(in.readLong());
            break;
         case FLOAT :
            value = new Float(in.readFloat());
            break;
         case DOUBLE :
            value = new Double(in.readDouble());
            break;
         case BOOLEAN :
            value = Primitives.valueOf(in.readBoolean());
            break;
         case STRING :
            if (longStrings)
            {
               //We cope with >= 64K Strings
               value = SafeUTF.instance.safeReadUTF(in);
            }
            else
            {
               //Limited to < 64K Strings
               value = in.readUTF();
            }
            break;
         case BYTES :
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            return bytes;
         case MAP:
         {
            Map m = readMap(in, false);
            if (m instanceof HashMap)
            {
               return (HashMap)m;
            }
            else
            {
               return new HashMap(m);
            }
         }            
         case LIST:
         {
            List l = readList(in);
            if (l instanceof ArrayList)
            {
               return (ArrayList)l;
            }
            else
            {
               return new ArrayList(l);
            }
         }
         case NULL:
            value = null;
            break;
         default :
            value = (Serializable)in.readObject();
      }
      return value;
   }
   
   protected static void internalWriteObject(ObjectOutput out, Serializable value,
                                             boolean containerTypes, boolean longStrings) throws IOException
   {
      // We cheat with some often used types - more efficient than using object serialization
      if (value == null)
      {
         out.writeByte(NULL);
      }
      else if (value instanceof String)
      {
         out.writeByte(STRING);
         if (longStrings)
         {
            //We can cope with >=64K Strings
            SafeUTF.instance.safeWriteUTF(out, (String)value);
         }
         else
         {
            //Limited to < 64K Strings
            out.writeUTF((String)value);
         }
      }
      else if (value instanceof Integer)
      {
         out.writeByte(INT);
         out.writeInt(((Integer) value).intValue());
      }
      else if (value instanceof Boolean)
      {
         out.writeByte(BOOLEAN);
         out.writeBoolean(((Boolean) value).booleanValue());
      }
      else if (value instanceof Byte)
      {
         out.writeByte(BYTE);
         out.writeByte(((Byte) value).byteValue());
      }
      else if (value instanceof Short)
      {
         out.writeByte(SHORT);
         out.writeShort(((Short) value).shortValue());
      }
      else if (value instanceof Long)
      {
         out.writeByte(LONG);
         out.writeLong(((Long) value).longValue());
      }
      else if (value instanceof Float)
      {
         out.writeByte(FLOAT);
         out.writeFloat(((Float) value).floatValue());
      }
      else if (value instanceof Double)
      {
         out.writeByte(DOUBLE);
         out.writeDouble(((Double) value).doubleValue());
      }
      else if (value instanceof byte[])
      {
         out.writeByte(BYTES);
         byte[] bytes = (byte[])value;
         out.writeInt(bytes.length);
         out.write(bytes);
      }      
      else if (containerTypes && value instanceof ArrayList)
      {
         out.write(LIST);
         writeList(out, (List)value);
      }
      else if (containerTypes && value instanceof HashMap)
      {
         out.write(MAP);
         writeMap(out, (Map)value, false);
      }
      else
      {
         // Default to standard serialization
         out.writeByte(OBJECT);
         out.writeObject(value);
      }
   }
   
   public static void writeMap(ObjectOutput out, Map map, boolean stringKeys) throws IOException
   {      
      if (map.isEmpty())
      {
         out.writeByte(NULL);
      }
      else
      {      
         out.writeByte(MAP);
         Set entrySet = map.entrySet();
         out.writeInt(entrySet.size());
         for (Iterator it = entrySet.iterator(); it.hasNext(); )
         {
            Map.Entry me = (Map.Entry)it.next();
            
            //Write the key
            if (stringKeys)
            {
               out.writeUTF((String)me.getKey());
            }
            else
            {
               if (!(me.getKey() instanceof Serializable))
               {
                  throw new IOException("Key in map must be Serializable: " + me.getKey());
               }
               internalWriteObject(out, (Serializable)me.getKey(), false, false);
            }

            Object value = me.getValue();
            if (value != null && !(value instanceof Serializable))
            {
               throw new IOException("Value in map must be Serializable: " + value);
            }
            
            // write the value
            internalWriteObject(out, (Serializable)value, false, false);
         }
      }
   }
   
   public static Map readMap(ObjectInput in, boolean stringKeys) throws IOException, ClassNotFoundException
   {
      byte b = in.readByte();
      if (b == NULL)
      {
         return new HashMap();
      }
      else
      {      
         int size = in.readInt();
         HashMap m = new HashMap(size);
         for (int i = 0; i < size; i++)
         {
            Object key;
            if (stringKeys)
            {
               key = in.readUTF();
            }
            else
            {
               key = internalReadObject(in, false);
            }
            
            Object value = internalReadObject(in, false);
            
            m.put(key, value);
         }
         return m;
      }
   }  
   
   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   protected long messageID;
   protected boolean reliable;
   /** GMT milliseconds at which this message expires. 0 means never expires **/
   protected long expiration;
   protected long timestamp;
   protected Map headers;
   protected boolean redelivered;
   protected byte priority;
   protected int deliveryCount;   

   // Constructors --------------------------------------------------

   /**
    * Required by externalization.
    */
   public RoutableSupport()
   {
   }

   /**
    * Constructs a generic Routable that is not reliable and does not expire.
    */
   public RoutableSupport(long messageID)
   {
      this(messageID, false, Long.MAX_VALUE);
   }

   /**
    * Constructs a generic Routable that does not expire.
    */
   public RoutableSupport(long messageID, boolean reliable)
   {
      this(messageID, reliable, Long.MAX_VALUE);
   }

   public RoutableSupport(long messageID, boolean reliable, long timeToLive)
   {
      this(messageID,
           reliable,
           timeToLive == Long.MAX_VALUE ? 0 : System.currentTimeMillis() + timeToLive,
           System.currentTimeMillis(),
           (byte)4,        
           0,
           null);
   }

   public RoutableSupport(long messageID,
                          boolean reliable, 
                          long expiration, 
                          long timestamp,
                          byte priority,
                          int deliveryCount,                          
                          Map headers)
   {
      this.messageID = messageID;
      this.reliable = reliable;
      this.expiration = expiration;
      this.timestamp = timestamp;
      this.priority = priority;
      this.deliveryCount = deliveryCount;
      this.redelivered = deliveryCount >= 1;      
      if (headers == null)
      {
         this.headers = new HashMap();
      }
      else
      {
         this.headers = new HashMap(headers);
      }
   }
   
   protected RoutableSupport(RoutableSupport other)
   {
      this.messageID = other.messageID;
      this.reliable = other.reliable;
      this.expiration = other.expiration;
      this.timestamp = other.timestamp;
      this.headers = new HashMap(other.headers);
      this.redelivered = other.redelivered;
      this.deliveryCount = other.deliveryCount;
      this.priority = other.priority;  
   }

   // Routable implementation ---------------------------------------

   public long getMessageID()
   {
      return messageID;
   }

   public boolean isReliable()
   {
      return reliable;
   }

   public long getExpiration()
   {
      return expiration;
   }

   public long getTimestamp()
   {
      return timestamp;
   }

   public boolean isRedelivered()
   {
      return redelivered;
   }

   public void setRedelivered(boolean redelivered)
   {
      this.redelivered = redelivered;      
   }
   
   public void setReliable(boolean reliable)
   {
      this.reliable = reliable;
   }
   
   public int getDeliveryCount()
   {
      return deliveryCount;
   }
   
   public void incrementDeliveryCount()
   {
      deliveryCount++;      
   }
   
   public void decrementDeliveryCount()
   {
      deliveryCount--;
   }
   
   public void setDeliveryCount(int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
      if (deliveryCount > 0)
      {
         this.redelivered = true;
      }
   }

   public Serializable putHeader(String name, Serializable value)
   {
      return (Serializable)headers.put(name, value);
   }

   public Serializable getHeader(String name)
   {
      return (Serializable)headers.get(name);
   }

   public Serializable removeHeader(String name)
   {
      return (Serializable)headers.remove(name);
   }

   public boolean containsHeader(String name)
   {
      return headers.containsKey(name);
   }

   public Set getHeaderNames()
   {
      return headers.keySet();
   }
   
   public Map getHeaders()
   {
      return headers;
   }
   
   public byte getPriority()
   {
      return priority;
   }
   
   public void setPriority(byte priority)
   {
      this.priority = priority;
   }
   
   // Externalizable implementation ---------------------------------
   
   public void writeExternal(ObjectOutput out) throws IOException
   {      
      out.writeLong(messageID);
      out.writeBoolean(reliable);
      out.writeLong(expiration);
      out.writeLong(timestamp);
      writeMap(out, headers, true);
      out.writeBoolean(redelivered);
      out.writeByte(priority);
      out.writeInt(deliveryCount);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {     
      messageID = in.readLong();
      reliable = in.readBoolean();
      expiration = in.readLong();
      timestamp = in.readLong();
      Map m = readMap(in, true);
      if (!(m instanceof HashMap))
      {
         headers =  new HashMap(m);
      }
      else
      {
         headers = (HashMap)m;
      }
      redelivered = in.readBoolean();
      priority = in.readByte();
      deliveryCount = in.readInt();
   }
   
   // Public --------------------------------------------------------

   public boolean isExpired()
   {
      if (expiration == 0)
      {
         return false;
      }
      long overtime = System.currentTimeMillis() - expiration;
      if (overtime >= 0)
      {
         // discard it
         if (trace)
         {
            log.trace("Message " + messageID + " expired by " + overtime + " ms");
         }
         
         return true;
      }
      return false;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("RoutableSupport[");
      sb.append(messageID);
      sb.append("]");
      return sb.toString();
   }

   // Protected -------------------------------------------------------
 
}
