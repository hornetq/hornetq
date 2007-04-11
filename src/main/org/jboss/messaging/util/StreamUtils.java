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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.jms.destination.JBossDestination;
import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;

/**
 * A StreamUtils
 *
 * Utility methods for reading and writing stuff to and from streams
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class StreamUtils
{
   private static final int BUFFER_SIZE = 4096;
   
   public static final byte NULL = 0;
   
   public static final byte STRING = 1;
   
   public static final byte MAP = 2;
   
   public static final byte BYTE = 3;

   public static final byte SHORT = 4;

   public static final byte INT = 5;

   public static final byte LONG = 6;

   public static final byte FLOAT = 7;

   public static final byte DOUBLE = 8;

   public static final byte BOOLEAN = 9;
   
   public static final byte BYTES = 10;
     
   public static final byte LIST = 11;
   
   public static final byte SERIALIZABLE = 12;
   
   public static final byte DESTINATION = 13;
   
   private static boolean useJBossSerialization = false;
   
   public static void setUseJBossSerialization(boolean use)
   {
      useJBossSerialization = use;
   }

         
   public static Object readObject(DataInputStream in, boolean longStrings)
      throws IOException, ClassNotFoundException
   {
      byte type = in.readByte();
      Object value = null;
      switch (type)
      {
         case NULL:
         {
            value = null;
            break;
         }
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
         case DESTINATION:
         {
            value = JBossDestination.readDestination(in);
            break;
         }
         case MAP:
         {
            value = readMap(in, false);
            break;
         } 
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
            value = in.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            break;         
         case BYTES :
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            value = bytes;
            break;                   
         case LIST:
         {
            value = readList(in);
            break;
         }
         case SERIALIZABLE:
         {
            ObjectInputStream ois;
            if (useJBossSerialization)
            {
               ois = new JBossObjectInputStream(in, Thread.currentThread().getContextClassLoader());
            }
            else
            {
               ois = new ObjectInputStreamWithClassLoader(in);
            }
                        
            value = ois.readObject();
            break;
         }              
         default :
         {
            throw new IllegalStateException("Unknown type: " + type);
         }
      }
      return value;
   }
   
   public static void writeObject(DataOutputStream out, Object object,
                                  boolean containerTypes, boolean longStrings) throws IOException
   {
      // more efficient than using object serialization for well known types
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
      else if (object instanceof JBossDestination)
      {
         out.writeByte(DESTINATION);
         JBossDestination.writeDestination(out, (JBossDestination)object);
      }
      else if (containerTypes && object instanceof Map)
      {
         out.writeByte(MAP);
         writeMap(out, (Map)object, false);
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
      else if (containerTypes && object instanceof List)
      {
         out.write(LIST);
         writeList(out, (List)object);
      }      
      else if (object instanceof Serializable)
      {
         out.writeByte(SERIALIZABLE);
         ObjectOutputStream oos;
         
         if (useJBossSerialization)
         {
            oos = new JBossObjectOutputStream(out);
         }
         else
         {
            oos = new ObjectOutputStream(out);
         }
                  
         oos.writeObject(object);
         oos.flush();
      }
      else
      {
         throw new IllegalArgumentException("Don't know how to deal with object " + object);
      }
   }  
   
   public static void writeList(DataOutputStream out, List list) throws IOException
   {
      out.writeInt(list.size());
      Iterator iter = list.iterator();
      while (iter.hasNext())
      {
         Object value = iter.next();
         writeObject(out, value, false, false);
      }
   }
   
   public static ArrayList readList(DataInputStream in) throws ClassNotFoundException, IOException
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
   
   public static void writeMap(DataOutputStream out, Map map, boolean stringKeys) throws IOException
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
            writeObject(out, me.getKey(), false, false);
         }
        
         // write the value
         writeObject(out, me.getValue(), false, false);
      }      
   }
   
   public static HashMap readMap(DataInputStream in, boolean stringKeys) throws IOException, ClassNotFoundException
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
   
   public static byte[] toBytes(Streamable streamable) throws Exception
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(BUFFER_SIZE);
      
      DataOutputStream daos = new DataOutputStream(baos);
      
      streamable.write(daos);
      
      daos.close();
      
      return baos.toByteArray();
   }
   
   public static void fromBytes(Streamable streamable, byte[] bytes) throws Exception
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      
      DataInputStream dais = new DataInputStream(bais);
      
      streamable.read(dais);
      
      dais.close();
   }
   
}
