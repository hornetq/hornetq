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

package org.hornetq.core.filter.impl;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.FilterConstants;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.ServerMessage;

/**
* This class implements a HornetQ filter
*
* @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
* @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
*
* HornetQ filters have the same syntax as JMS 1.1 selectors, but the identifiers are different.
*
* Valid identifiers that can be used are:
*
* HQPriority - the priority of the message
* HQTimestamp - the timestamp of the message
* HQDurable - "DURABLE" or "NON_DURABLE"
* HQExpiration - the expiration of the message
* HQSize - the encoded size of the full message in bytes
* HQUserID - the user specified ID string (if any)
* Any other identifers that appear in a filter expression represent header values for the message
*
* String values must be set as <code>SimpleString</code>, not <code>java.lang.String</code> (see JBMESSAGING-1307).
* Derived from JBoss MQ version by
*
* @author <a href="mailto:Norbert.Lataille@m4x.org">Norbert Lataille</a>
* @author <a href="mailto:jplindfo@helsinki.fi">Juha Lindfors</a>
* @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
* @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
* @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
*
* @version    $Revision: 3569 $
*
* $Id: Selector.java 3569 2008-01-15 21:14:04Z timfox $
*/
public class FilterImpl implements Filter
{

   // Constants -----------------------------------------------------

   private final SimpleString sfilterString;

   private final Map<SimpleString, Identifier> identifiers = new HashMap<SimpleString, Identifier>();

   private final FilterParser parser = new FilterParser();

   private final Object result;

   private final Class<? extends Object> resultType;

   // Static ---------------------------------------------------------

   /**
    * @return null if <code>filterStr</code> is null or an empty String and a valid filter else
    * @throws HornetQException if the string does not correspond to a valid filter
    */
   public static Filter createFilter(final String filterStr) throws HornetQException
   {
      return FilterImpl.createFilter(SimpleString.toSimpleString(filterStr == null ? null : filterStr.trim()));
   }

   /**
    * @return null if <code>filterStr</code> is null or an empty String and a valid filter else
    * @throws HornetQException if the string does not correspond to a valid filter
    */
   public static Filter createFilter(final SimpleString filterStr) throws HornetQException
   {
      if (filterStr == null || filterStr.length() == 0)
      {
         return null;
      }
      else
      {
         return new FilterImpl(filterStr);
      }
   }

   // Constructors ---------------------------------------------------

   private FilterImpl(final SimpleString str) throws HornetQException
   {
      sfilterString = str;

      try
      {
         result = parser.parse(sfilterString, identifiers);

         resultType = result.getClass();
      }
      catch (Throwable e)
      {
         HornetQLogger.LOGGER.invalidFilter(e, str);

         throw HornetQMessageBundle.BUNDLE.invalidFilter(e, str);
      }
   }

   // Filter implementation ---------------------------------------------------------------------

   public SimpleString getFilterString()
   {
      return sfilterString;
   }

   public boolean match(final ServerMessage message)
   {
      try
      {
         // Set the identifiers values

         for (Identifier id : identifiers.values())
         {
            Object val = null;

            if (id.getName().startsWith(FilterConstants.HORNETQ_PREFIX))
            {
               // Look it up as header fields
               val = getHeaderFieldValue(message, id.getName());
            }

            if (val == null)
            {
               val = message.getObjectProperty(id.getName());
            }

            id.setValue(val);

         }

         if (resultType.equals(Identifier.class))
         {
            return (Boolean)((Identifier)result).getValue();
         }
         else if (resultType.equals(Operator.class))
         {
            Operator op = (Operator)result;
            return (Boolean)op.apply();
         }
         else
         {
            throw new Exception("Bad object type: " + result);
         }
      }
      catch (Exception e)
      {
         HornetQLogger.LOGGER.invalidFilter(e, sfilterString);

         return false;
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((sfilterString == null) ? 0 : sfilterString.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      FilterImpl other = (FilterImpl)obj;
      if (sfilterString == null)
      {
         if (other.sfilterString != null)
            return false;
      }
      else if (!sfilterString.equals(other.sfilterString))
         return false;
      return true;
   }

   @Override
   public String toString()
   {
      return "FilterImpl [sfilterString=" + sfilterString + "]";
   }

   // Private --------------------------------------------------------------------------

   private Object getHeaderFieldValue(final ServerMessage msg, final SimpleString fieldName)
   {
      if (FilterConstants.HORNETQ_USERID.equals(fieldName))
      {
         // It's the stringified (hex) representation of a user id that can be used in a selector expression
         return new SimpleString("ID:" + msg.getUserID());
      }
      else if (FilterConstants.HORNETQ_PRIORITY.equals(fieldName))
      {
         return Integer.valueOf(msg.getPriority());
      }
      else if (FilterConstants.HORNETQ_TIMESTAMP.equals(fieldName))
      {
         return msg.getTimestamp();
      }
      else if (FilterConstants.HORNETQ_DURABLE.equals(fieldName))
      {
         return msg.isDurable() ? FilterConstants.DURABLE : FilterConstants.NON_DURABLE;
      }
      else if (FilterConstants.HORNETQ_EXPIRATION.equals(fieldName))
      {
         return msg.getExpiration();
      }
      else if (FilterConstants.HORNETQ_SIZE.equals(fieldName))
      {
         return msg.getEncodeSize();
      }
      else
      {
         return null;
      }
   }
}
