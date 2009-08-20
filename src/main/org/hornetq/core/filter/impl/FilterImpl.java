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

import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.SimpleString;
import org.hornetq.core.filter.impl.FilterParser;

/**
* This class implements a JBoss Messaging filter
* 
* @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
* @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
* 
* JBM filters have the same syntax as JMS 1.1 selectors, but the identifiers are different.
* 
* Valid identifiers that can be used are:
* 
* JBMessageID - the message id of the message
* JBMPriority - the priority of the message
* JBMTimestamp - the timestamp of the message
* JBMDurable - "DURABLE" or "NON_DURABLE"
* JBMExpiration - the expiration of the message
* JBMSize - the encoded size of the full message in bytes
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

   private static final Logger log = Logger.getLogger(FilterImpl.class);

   private static final SimpleString JBM_EXPIRATION = new SimpleString("JBMExpiration");

   private static final SimpleString JBM_DURABLE = new SimpleString("JBMDurable");

   private static final SimpleString NON_DURABLE = new SimpleString("NON_DURABLE");

   private static final SimpleString DURABLE = new SimpleString("DURABLE");

   private static final SimpleString JBM_TIMESTAMP = new SimpleString("JBMTimestamp");

   private static final SimpleString JBM_PRIORITY = new SimpleString("JBMPriority");

   private static final SimpleString JBM_SIZE = new SimpleString("JBMSize");

   private static final SimpleString JBM_PREFIX = new SimpleString("JBM");

   // Attributes -----------------------------------------------------

   private final SimpleString sfilterString;

   private final Map<SimpleString, Identifier> identifiers = new HashMap<SimpleString, Identifier>();

   private final Operator operator;

   private final FilterParser parser = new FilterParser();

   // Static ---------------------------------------------------------

   /**
    * @return null if <code>filterStr</code> is null or a valid filter else
    * @throws MessagingException if the string does not correspond to a valid filter
    */
   public static Filter createFilter(final String filterStr) throws MessagingException
   {
      Filter filter = filterStr == null ? null : new FilterImpl(new SimpleString(filterStr));
      return filter;
   }
   
   /**
    * @return null if <code>filterStr</code> is null or a valid filter else
    * @throws MessagingException if the string does not correspond to a valid filter
    */
   public static Filter createFilter(final SimpleString filterStr) throws MessagingException
   {
      Filter filter = filterStr == null ? null : new FilterImpl(filterStr);
      return filter;
   }

   // Constructors ---------------------------------------------------

   public FilterImpl(final SimpleString str) throws MessagingException
   {
      sfilterString = str;

      try
      {
         operator = (Operator)parser.parse(sfilterString, identifiers);
      }
      catch (Throwable e)
      {
         log.error("Invalid filter", e);
         
         throw new MessagingException(MessagingException.INVALID_FILTER_EXPRESSION, "Invalid filter: " + sfilterString);
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

            if (id.getName().startsWith(JBM_PREFIX))
            {               
               // Look it up as header fields
               val = getHeaderFieldValue(message, id.getName());
            }

            if (val == null)
            {               
               val = message.getProperty(id.getName());               
            }

            id.setValue(val);

         }
         
         // Compute the result of this operator
         
         boolean res = (Boolean)operator.apply();

         return res;
      }
      catch (Exception e)
      {
         log.warn("Invalid filter string: " + sfilterString, e);

         return false;
      }
   }

   // Private --------------------------------------------------------------------------

   private Object getHeaderFieldValue(final ServerMessage msg, final SimpleString fieldName)
   {
      if (JBM_PRIORITY.equals(fieldName))
      {
         return new Integer(msg.getPriority());
      }
      else if (JBM_TIMESTAMP.equals(fieldName))
      {
         return msg.getTimestamp();
      }
      else if (JBM_DURABLE.equals(fieldName))
      {
         return msg.isDurable() ? DURABLE : NON_DURABLE;
      }
      else if (JBM_EXPIRATION.equals(fieldName))
      {
         return msg.getExpiration();
      }
      else if (JBM_SIZE.equals(fieldName))
      {
         return msg.getEncodeSize();
      }
      else
      {
         return null;
      }
   }
}
