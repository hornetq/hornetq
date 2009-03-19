/*
 * JBoss, Home of Professional Open Source Copyright 2008, Red Hat Middleware
 * LLC, and individual contributors by the @authors tag. See the copyright.txt
 * in the distribution for a full listing of individual contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.client.management.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.utils.SimpleString;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementHelper
{

   // Constants -----------------------------------------------------

   public static final SimpleString HDR_JMX_OBJECTNAME = new SimpleString("_JBM_JMX_ObjectName");

   public static final SimpleString HDR_JMX_ATTRIBUTE = new SimpleString("_JBM_JMXAttribute");

   public static final SimpleString HDR_JMX_OPERATION_PREFIX = new SimpleString("_JBM_JMXOperation$");

   public static final SimpleString HDR_JMX_OPERATION_NAME = new SimpleString(HDR_JMX_OPERATION_PREFIX + "name");

   public static final SimpleString HDR_JMX_OPERATION_SUCCEEDED = new SimpleString("_JBM_JMXOperationSucceeded");

   public static final SimpleString HDR_JMX_OPERATION_EXCEPTION = new SimpleString("_JBM_JMXOperationException");

   public static final SimpleString HDR_NOTIFICATION_TYPE = new SimpleString("_JBM_NotifType");

   public static final SimpleString HDR_NOTIFICATION_TIMESTAMP = new SimpleString("_JBM_NotifTimestamp");

   public static final SimpleString HDR_ROUTING_NAME = new SimpleString("_JBM_RoutingName");

   public static final SimpleString HDR_CLUSTER_NAME = new SimpleString("_JBM_ClusterName");

   public static final SimpleString HDR_ADDRESS = new SimpleString("_JBM_Address");

   public static final SimpleString HDR_BINDING_ID = new SimpleString("_JBM_Binding_ID");

   public static final SimpleString HDR_BINDING_TYPE = new SimpleString("_JBM_Binding_Type");

   public static final SimpleString HDR_FILTERSTRING = new SimpleString("_JBM_FilterString");

   public static final SimpleString HDR_DISTANCE = new SimpleString("_JBM_Distance");

   private static final SimpleString NULL = new SimpleString("_JBM_NULL");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void putAttribute(final Message message, final ObjectName objectName, final String attribute)
   {
      message.putStringProperty(HDR_JMX_OBJECTNAME, new SimpleString(objectName.toString()));
      message.putStringProperty(HDR_JMX_ATTRIBUTE, new SimpleString(attribute));
   }

   public static void putOperationInvocation(final Message message,
                                             final ObjectName objectName,
                                             final String operationName,
                                             final Object... parameters)
   {
      // store the name of the operation...
      message.putStringProperty(HDR_JMX_OBJECTNAME, new SimpleString(objectName.toString()));
      message.putStringProperty(HDR_JMX_OPERATION_NAME, new SimpleString(operationName));
      // ... and all the parameters (preserving their types)
      for (int i = 0; i < parameters.length; i++)
      {
         Object parameter = parameters[i];
         // use a zero-filled 2-padded index:
         // if there is more than 10 parameters, order is preserved (e.g. 02 will be before 10)
         SimpleString key = new SimpleString(String.format("%s%02d", HDR_JMX_OPERATION_PREFIX, i));
         storeTypedProperty(message, key, parameter);
      }
   }

   public static List<Object> retrieveOperationParameters(final Message message)
   {
      List<Object> params = new ArrayList<Object>();
      Set<SimpleString> propertyNames = message.getPropertyNames();
      // put the property names in a list to sort them and have the parameters
      // in the correct order
      List<SimpleString> propsNames = new ArrayList<SimpleString>(propertyNames);
      Collections.sort(propsNames);
      for (SimpleString propertyName : propsNames)
      {
         if (propertyName.startsWith(ManagementHelper.HDR_JMX_OPERATION_PREFIX))
         {
            String s = propertyName.toString();
            // split by the dot
            String[] ss = s.split("\\$");
            try
            {
               int index = Integer.parseInt(ss[ss.length - 1]);
               Object value = message.getProperty(propertyName);
               if (value instanceof SimpleString)
               {
                  value = value.toString();
               }
               if (NULL.toString().equals(value))
               {
                  value = null;
               }
               params.add(index, value);
            }
            catch (NumberFormatException e)
            {
               // ignore the property (it is the operation name)
            }
         }
      }
      return params;
   }

   public static boolean isOperationResult(final Message message)
   {
      return message.containsProperty(HDR_JMX_OPERATION_SUCCEEDED);
   }

   public static boolean isAttributesResult(final Message message)
   {
      return !(isOperationResult(message));
   }

   public static void storeResult(final Message message, final Object result)
   {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try
      {
         ObjectOutputStream oos = new ObjectOutputStream(baos);
         oos.writeObject(result);
      }
      catch (IOException e)
      {
         throw new IllegalStateException(result + " can not be written to a byte array");
      }
      byte[] data = baos.toByteArray();
      message.getBody().writeInt(data.length);
      message.getBody().writeBytes(data);
   }

   public static Object getResult(final Message message)
   {
      int len = message.getBody().readInt();
      byte[] data = new byte[len];
      message.getBody().readBytes(data);
      return from(data);
   }

   public static boolean hasOperationSucceeded(final Message message)
   {
      if (!isOperationResult(message))
      {
         return false;
      }
      if (message.containsProperty(HDR_JMX_OPERATION_SUCCEEDED))
      {
         return (Boolean)message.getProperty(HDR_JMX_OPERATION_SUCCEEDED);
      }
      return false;
   }

   public static String getOperationExceptionMessage(final Message message)
   {
      if (message.containsProperty(HDR_JMX_OPERATION_EXCEPTION))
      {
         return ((SimpleString)message.getProperty(HDR_JMX_OPERATION_EXCEPTION)).toString();
      }
      return null;
   }

   public static void storeTypedProperty(final Message message, final SimpleString key, final Object typedProperty)
   {
      if (typedProperty == null)
      {
         message.putStringProperty(key, NULL);
         return;
      }
      
      checkSimpleType(typedProperty);

      if (typedProperty instanceof Void)
      {
         // do not put the returned value if the operation was a procedure
      }
      else if (typedProperty instanceof Boolean)
      {
         message.putBooleanProperty(key, (Boolean)typedProperty);
      }
      else if (typedProperty instanceof Byte)
      {
         message.putByteProperty(key, (Byte)typedProperty);
      }
      else if (typedProperty instanceof Short)
      {
         message.putShortProperty(key, (Short)typedProperty);
      }
      else if (typedProperty instanceof Integer)
      {
         message.putIntProperty(key, (Integer)typedProperty);
      }
      else if (typedProperty instanceof Long)
      {
         message.putLongProperty(key, (Long)typedProperty);
      }
      else if (typedProperty instanceof Float)
      {
         message.putFloatProperty(key, (Float)typedProperty);
      }
      else if (typedProperty instanceof Double)
      {
         message.putDoubleProperty(key, (Double)typedProperty);
      }
      else if (typedProperty instanceof String)
      {
         message.putStringProperty(key, new SimpleString((String)typedProperty));
      }
      // serialize as a SimpleString
      else
      {
         message.putStringProperty(key, new SimpleString("" + typedProperty));
      }
   }

   private static void checkSimpleType(Object o)
   {
      if (!((o instanceof Void) || (o instanceof Boolean) ||
            (o instanceof Byte) ||
            (o instanceof Short) ||
            (o instanceof Integer) ||
            (o instanceof Long) ||
            (o instanceof Float) ||
            (o instanceof Double) ||
            (o instanceof String) || (o instanceof SimpleString)))
      {
         throw new IllegalStateException("Can not store object as a message property: " + o);
      }
   }

   public static Object from(final byte[] bytes)
   {
      ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
      ObjectInputStream ois;
      try
      {
         ois = new ObjectInputStream(bais);
         return ois.readObject();
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
