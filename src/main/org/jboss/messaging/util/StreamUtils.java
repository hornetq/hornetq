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
package org.jboss.messaging.util;

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

import org.jboss.util.Primitives;

/**
 * A StreamUtils
 *
 * Utility methods for reading and writing stuff to and from streams
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class StreamUtils
{
   protected static final byte BYTE = 0;

   protected static final byte SHORT = 1;

   protected static final byte INT = 2;

   protected static final byte LONG = 3;

   protected static final byte FLOAT = 4;

   protected static final byte DOUBLE = 5;

   protected static final byte BOOLEAN = 6;

   protected static final byte STRING = 7;

   protected static final byte SERIALIZABLE = 8;

   protected static final byte NULL = 9;
   
   protected static final byte BYTES = 10;
   
   protected static final byte MAP = 11;
   
   protected static final byte LIST = 12;
   
   protected static final byte NOT_NULL = 13;
         
   public static Object readObject(ObjectInput in, boolean longStrings)
      throws IOException, ClassNotFoundException
   {
      byte type = in.readByte();
      Object value = null;
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
            value = bytes;
            break;
         case MAP:
         {
            value = readMap(in, false);
            break;
         }            
         case LIST:
         {
            value = readList(in);
            break;
         }
         case SERIALIZABLE:
         {
            value = (Serializable)in.readObject();
            break;
         }                  
         case NULL:
         {
            value = null;
            break;
         }
         default :
         {
            throw new IllegalStateException("Unknown type: " + type);
         }
      }
      return value;
   }
   
   public static void writeObject(ObjectOutput out, Object object,
                                  boolean containerTypes, boolean longStrings) throws IOException
   {
      // We cheat with some often used types - more efficient than using object serialization
      if (object == null)
      {
         out.writeByte(NULL);
      }
      else if (object instanceof String)
      {
         out.writeByte(STRING);
         if (longStrings)
         {
            //We can cope with >=64K Strings
            SafeUTF.instance.safeWriteUTF(out, (String)object);
         }
         else
         {
            //Limited to < 64K Strings
            out.writeUTF((String)object);
         }
      }
      else if (object instanceof Integer)
      {
         out.writeByte(INT);
         out.writeInt(((Integer) object).intValue());
      }
      else if (object instanceof Boolean)
      {
         out.writeByte(BOOLEAN);
         out.writeBoolean(((Boolean) object).booleanValue());
      }
      else if (object instanceof Byte)
      {
         out.writeByte(BYTE);
         out.writeByte(((Byte) object).byteValue());
      }
      else if (object instanceof Short)
      {
         out.writeByte(SHORT);
         out.writeShort(((Short) object).shortValue());
      }
      else if (object instanceof Long)
      {
         out.writeByte(LONG);
         out.writeLong(((Long) object).longValue());
      }
      else if (object instanceof Float)
      {
         out.writeByte(FLOAT);
         out.writeFloat(((Float) object).floatValue());
      }
      else if (object instanceof Double)
      {
         out.writeByte(DOUBLE);
         out.writeDouble(((Double) object).doubleValue());
      }
      else if (object instanceof byte[])
      {
         out.writeByte(BYTES);
         byte[] bytes = (byte[])object;
         out.writeInt(bytes.length);
         out.write(bytes);
      }      
      else if (containerTypes && object instanceof ArrayList)
      {
         out.write(LIST);
         writeList(out, (List)object);
      }
      else if (containerTypes && object instanceof HashMap)
      {
         out.write(MAP);
         writeMap(out, (Map)object, false);
      }
      else if (object instanceof Serializable)
      {
         out.writeByte(SERIALIZABLE);
         out.writeObject(object);
      }
      else
      {
         throw new IllegalArgumentException("Don't know how to deal with object " + object);
      }
   }  
   
   public static void writeList(ObjectOutput out, List list) throws IOException
   {
      out.writeInt(list.size());
      Iterator iter = list.iterator();
      while (iter.hasNext())
      {
         Object value = iter.next();
         writeObject(out, value, false, false);
      }
   }
   
   public static ArrayList readList(ObjectInput in) throws ClassNotFoundException, IOException
   {
      int size = in.readInt();
      ArrayList list = new ArrayList(size);
      for (int i = 0; i < size; i++)
      {
         Object obj = readObject(in, false);
         list.add(obj);
      }
      return list;
   }
   
   public static void writeMap(ObjectOutput out, Map map, boolean stringKeys) throws IOException
   {      
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
            writeObject(out, (Serializable)me.getKey(), false, false);
         }

         Object value = me.getValue();
         if (value != null && !(value instanceof Serializable))
         {
            throw new IOException("Value in map must be Serializable: " + value);
         }
         
         // write the value
         writeObject(out, (Serializable)value, false, false);
      }      
   }
   
   public static HashMap readMap(ObjectInput in, boolean stringKeys) throws IOException, ClassNotFoundException
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
            key = readObject(in, false);
         }
         
         Object value = readObject(in, false);
         
         m.put(key, value);
      }
      return m;      
   }  
}
