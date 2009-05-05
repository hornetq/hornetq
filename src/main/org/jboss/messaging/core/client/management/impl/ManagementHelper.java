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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.json.JSONArray;
import org.jboss.messaging.utils.json.JSONObject;

/*
 * 
 * Operation params and results are encoded as JSON arrays
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class ManagementHelper
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ManagementHelper.class);

   public static final SimpleString HDR_RESOURCE_NAME = new SimpleString("_JBM_ResourceName");

   public static final SimpleString HDR_ATTRIBUTE = new SimpleString("_JBM_Attribute");

   public static final SimpleString HDR_OPERATION_NAME = new SimpleString("_JBM_OperationName");

   public static final SimpleString HDR_OPERATION_SUCCEEDED = new SimpleString("_JBM_OperationSucceeded");

   public static final SimpleString HDR_NOTIFICATION_TYPE = new SimpleString("_JBM_NotifType");

   public static final SimpleString HDR_NOTIFICATION_TIMESTAMP = new SimpleString("_JBM_NotifTimestamp");

   public static final SimpleString HDR_ROUTING_NAME = new SimpleString("_JBM_RoutingName");

   public static final SimpleString HDR_CLUSTER_NAME = new SimpleString("_JBM_ClusterName");

   public static final SimpleString HDR_ADDRESS = new SimpleString("_JBM_Address");

   public static final SimpleString HDR_BINDING_ID = new SimpleString("_JBM_Binding_ID");

   public static final SimpleString HDR_BINDING_TYPE = new SimpleString("_JBM_Binding_Type");

   public static final SimpleString HDR_FILTERSTRING = new SimpleString("_JBM_FilterString");

   public static final SimpleString HDR_DISTANCE = new SimpleString("_JBM_Distance");

   public static final SimpleString HDR_CONSUMER_COUNT = new SimpleString("_JBM_ConsumerCount");

   public static final SimpleString HDR_USER = new SimpleString("_JBM_User");

   public static final SimpleString HDR_CHECK_TYPE = new SimpleString("_JBM_CheckType");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static void putAttribute(final Message message, final String resourceName, final String attribute)
   {
      message.putStringProperty(HDR_RESOURCE_NAME, new SimpleString(resourceName));
      message.putStringProperty(HDR_ATTRIBUTE, new SimpleString(attribute));
   }

   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName) throws Exception
   {
      putOperationInvocation(message, resourceName, operationName, (Object[])null);
   }

   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName,
                                             final Object... parameters) throws Exception
   {
      // store the name of the operation in the headers
      message.putStringProperty(HDR_RESOURCE_NAME, new SimpleString(resourceName));
      message.putStringProperty(HDR_OPERATION_NAME, new SimpleString(operationName));

      // and the params go in the body, since might be too large for header

      String paramString;

      if (parameters != null)
      {
         JSONArray jsonArray = toJSONArray(parameters);

         paramString = jsonArray.toString();
      }
      else
      {
         paramString = null;
      }

      message.getBody().writeNullableString(paramString);
   }

   private static JSONArray toJSONArray(final Object[] array) throws Exception
   {
      JSONArray jsonArray = new JSONArray();
      
      for (int i = 0; i < array.length; i++)
      {
         Object parameter = array[i];
         
         if (parameter instanceof Map)
         {
            Map<String, Object> map = (Map<String, Object>)parameter;

            JSONObject jsonObject = new JSONObject();

            for (Map.Entry<String, Object> entry : map.entrySet())
            {
               String key = entry.getKey();

               Object val = entry.getValue();

               if (val != null)
               {
                  if (val.getClass().isArray())
                  {                   
                     val = toJSONArray((Object[])val);
                  }
                  else
                  {
                     checkType(val);
                  }
               }

               jsonObject.put(key, val);
            }

            jsonArray.put(jsonObject);
         }
         else
         {
            if (parameter != null)
            {
               Class clz = parameter.getClass();
   
               if (clz.isArray())
               {
                  Object[] innerArray = (Object[])parameter;
   
                  jsonArray.put(toJSONArray(innerArray));
               }
               else
               {
                  checkType(parameter);
   
                  jsonArray.put(parameter);
               }
            }
            else
            {
               jsonArray.put((Object)null);
            }
         }
      }

      return jsonArray;
   }

   private static Object[] fromJSONArray(final JSONArray jsonArray) throws Exception
   {
      Object[] array = new Object[jsonArray.length()];

      for (int i = 0; i < jsonArray.length(); i++)
      {
         Object val = jsonArray.get(i);

         if (val instanceof JSONArray)
         {
            Object[] inner = fromJSONArray((JSONArray)val);

            array[i] = inner;
         }
         else if (val instanceof JSONObject)
         {
            JSONObject jsonObject = (JSONObject)val;

            Map<String, Object> map = new HashMap<String, Object>();

            Iterator<String> iter = jsonObject.keys();
            
            while (iter.hasNext())
            {
               String key = iter.next();
               
               Object innerVal = jsonObject.get(key);
               
               if (innerVal instanceof JSONArray)
               {
                  innerVal = fromJSONArray(((JSONArray)innerVal));
               }

               map.put(key, innerVal);
            }

            array[i] = map;
         }
         else
         {
            array[i] = val;
         }
      }

      return array;
   }

   private static void checkType(final Object param)
   {
      if (param instanceof Integer == false && param instanceof Long == false &&
          param instanceof Double == false &&
          param instanceof String == false &&
          param instanceof Boolean == false &&
          param instanceof Byte == false &&
          param instanceof Short == false)
      {
         throw new IllegalArgumentException("Params for management operations must be of the following type: " + "int long double String boolean Map or array thereof " +
                                            " but found " + param.getClass().getName());
      }
   }

   public static Object[] retrieveOperationParameters(final Message message) throws Exception
   {
      String jsonString = message.getBody().readNullableString();

      if (jsonString != null)
      {
         JSONArray jsonArray = new JSONArray(jsonString);

         return fromJSONArray(jsonArray);
      }
      else
      {
         return null;
      }
   }

   public static boolean isOperationResult(final Message message)
   {
      return message.containsProperty(HDR_OPERATION_SUCCEEDED);
   }

   public static boolean isAttributesResult(final Message message)
   {
      return !(isOperationResult(message));
   }

   public static void storeResult(final Message message, final Object result) throws Exception
   {
      String resultString;

      if (result != null)
      {
         // Result is stored in body, also encoded as JSON array of length 1

         JSONArray jsonArray = toJSONArray(new Object[] { result });

         resultString = jsonArray.toString();                  
      }
      else
      {
         resultString = null;
      }

      message.getBody().writeNullableString(resultString);
   }

   public static Object[] getResults(final Message message) throws Exception
   {
      String jsonString = message.getBody().readNullableString();
      
      if (jsonString != null)
      {
         JSONArray jsonArray = new JSONArray(jsonString);

         Object[] res = fromJSONArray(jsonArray);

         return res;
      }
      else
      {
         return null;
      }
   }

   public static Object getResult(final Message message) throws Exception
   {
      Object[] res = getResults(message);

      if (res != null)
      {
         return res[0];
      }
      else
      {
         return null;
      }
   }

   public static boolean hasOperationSucceeded(final Message message)
   {
      if (!isOperationResult(message))
      {
         return false;
      }
      if (message.containsProperty(HDR_OPERATION_SUCCEEDED))
      {
         return (Boolean)message.getProperty(HDR_OPERATION_SUCCEEDED);
      }
      return false;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
