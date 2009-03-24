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
package org.jboss.messaging.core.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.UUIDGenerator;

/**
 * A TransportConfiguration
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class TransportConfiguration implements Serializable
{
   private static final long serialVersionUID = -3994528421527392679L;

   private String name;

   private String factoryClassName;

   private Map<String, Object> params;

   private static final byte TYPE_BOOLEAN = 0;

   private static final byte TYPE_INT = 1;

   private static final byte TYPE_LONG = 2;

   private static final byte TYPE_STRING = 3;

   public void encode(final MessagingBuffer buffer)
   {
      buffer.writeString(name);
      buffer.writeString(factoryClassName);

      buffer.writeInt(params == null ? 0 : params.size());

      if (params != null)
      {
         for (Map.Entry<String, Object> entry : params.entrySet())
         {
            buffer.writeString(entry.getKey());

            Object val = entry.getValue();

            if (val instanceof Boolean)
            {
               buffer.writeByte(TYPE_BOOLEAN);
               buffer.writeBoolean((Boolean)val);
            }
            else if (val instanceof Integer)
            {
               buffer.writeByte(TYPE_INT);
               buffer.writeInt((Integer)val);
            }
            else if (val instanceof Long)
            {
               buffer.writeByte(TYPE_LONG);
               buffer.writeLong((Long)val);
            }
            else if (val instanceof String)
            {
               buffer.writeByte(TYPE_STRING);
               buffer.writeString((String)val);
            }
            else
            {
               throw new IllegalArgumentException("Invalid type " + val);
            }
         }
      }
   }

   public void decode(final MessagingBuffer buffer)
   {
      name = buffer.readString();
      factoryClassName = buffer.readString();

      int num = buffer.readInt();

      if (params == null)
      {
         if (num > 0)
         {
            params = new HashMap<String, Object>();
         }
      }
      else
      {
         params.clear();
      }

      for (int i = 0; i < num; i++)
      {
         String key = buffer.readString();

         byte type = buffer.readByte();

         Object val;

         switch (type)
         {
            case TYPE_BOOLEAN:
            {
               val = buffer.readBoolean();

               break;
            }
            case TYPE_INT:
            {
               val = buffer.readInt();

               break;
            }
            case TYPE_LONG:
            {
               val = buffer.readLong();

               break;
            }
            case TYPE_STRING:
            {
               val = buffer.readString();

               break;
            }
            default:
            {
               throw new IllegalArgumentException("Invalid type " + type);
            }
         }

         params.put(key, val);
      }
   }

   public static String[] splitHosts(final String commaSeparatedHosts)
   {
      if (commaSeparatedHosts == null)
      {
         return new String[0];
      }
      String[] hosts = commaSeparatedHosts.split(",");

      for (int i = 0; i < hosts.length; i++)
      {
         hosts[i] = hosts[i].trim();
      }
      return hosts;
   }

   public TransportConfiguration()
   {
   }

   public TransportConfiguration(final String className, final Map<String, Object> params, final String name)
   {
      this.factoryClassName = className;

      this.params = params;

      this.name = name;
   }

   public TransportConfiguration(final String className, final Map<String, Object> params)
   {
      this(className, params, UUIDGenerator.getInstance().generateStringUUID());
   }

   public TransportConfiguration(final String className)
   {
      this(className, new HashMap<String, Object>(), UUIDGenerator.getInstance().generateStringUUID());
   }

   public String getName()
   {
      return name;
   }

   public String getFactoryClassName()
   {
      return factoryClassName;
   }

   public Map<String, Object> getParams()
   {
      return params;
   }

   public int hashCode()
   {
      return factoryClassName.hashCode();
   }

   public boolean equals(final Object other)
   {
      if (other instanceof TransportConfiguration == false)
      {
         return false;
      }

      TransportConfiguration kother = (TransportConfiguration)other;

      if (factoryClassName.equals(kother.factoryClassName))
      {
         if (params == null)
         {
            return kother.params == null;
         }
         else
         {
            if (kother.params == null)
            {
               return false;
            }
            else if (params.size() == kother.params.size())
            {
               for (Map.Entry<String, Object> entry : params.entrySet())
               {
                  Object thisVal = entry.getValue();

                  Object otherVal = kother.params.get(entry.getKey());

                  if (otherVal == null || !otherVal.equals(thisVal))
                  {
                     return false;
                  }
               }
               return true;
            }
            else
            {
               return false;
            }
         }
      }
      else
      {
         return false;
      }
   }
}
