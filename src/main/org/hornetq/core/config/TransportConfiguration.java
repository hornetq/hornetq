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
package org.hornetq.core.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.utils.UUIDGenerator;

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
   
   public String toString()
   {
      StringBuilder str = new StringBuilder(replaceWildcardChars(factoryClassName));

      if (params != null)
      {
         if (!params.isEmpty())
         {
            str.append("?");
         }

         boolean first = true;
         for (Map.Entry<String, Object> entry : params.entrySet())
         {
            if (!first)
            {
               str.append("&");
            }
            String encodedKey = replaceWildcardChars(entry.getKey());

            String val = entry.getValue().toString();
            String encodedVal = replaceWildcardChars(val);

            str.append(encodedKey).append('=').append(encodedVal);

            first = false;
         }
      }

      return str.toString();
   }
   
   public void encode(final HornetQBuffer buffer)
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

   public void decode(final HornetQBuffer buffer)
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
   
   private String replaceWildcardChars(final String str)
   {
      return str.replace('.', '-');
   }
}
