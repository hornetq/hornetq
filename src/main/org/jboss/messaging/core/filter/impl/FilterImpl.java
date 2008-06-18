/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.filter.impl;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.util.SimpleString;

/**
* This class implements a JBoss Messaging filter
* 
* @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
* Any other identifers that appear in a filter expression represent header values for the message
* 
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
  private static final Logger log = Logger.getLogger(FilterImpl.class);
  
  private final SimpleString sfilterString;
  
  private final String filterString;

  private final Map<String, Identifier> identifiers = new HashMap<String, Identifier>();
  
  private final Operator operator;
  
  private final FilterParser parser = new FilterParser();
  
  //TODO - convert to work natively with SimpleString
  public FilterImpl(final SimpleString str) throws MessagingException
  {
     this.filterString = str == null ? null : str.toString();
     this.sfilterString = str;

     try
     {
        operator = (Operator)parser.parse(filterString, identifiers);
     }
     catch (Throwable e)
     {   	  
        throw new MessagingException(MessagingException.INVALID_FILTER_EXPRESSION, "Invalid filter: " + filterString);
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
           
           if (id.getName().startsWith("JBM"))
           {           
              //Look it up as header fields
              
              val = getHeaderFieldValue(message, id.getName());
           }
                     
           if (val == null)
           {
              //First look it up in the headers
              
              //TODO - speed this up to avoid conversion
              //Filter should be refactored to deal with SimpleString natively
              val = message.getProperty(new SimpleString(id.getName()));             
              
              if (val instanceof SimpleString)
              {
                 val = ((SimpleString)val).toString();
              }
           }
           
           if (val != null)
           {
              id.setValue(val);
           }
        }
        
        // Compute the result of this operator
        
        boolean res = (Boolean)operator.apply();
        
        return res;
     }
     catch (Exception e)
     {
        log.warn("Invalid filter string: " + filterString, e);
        
        return false;
     }
  }
  
  // Private --------------------------------------------------------------------------
 
  private Object getHeaderFieldValue(final ServerMessage msg, final String fieldName)
  {
     if ("JBMMessageID".equals(fieldName))
     {
        return msg.getMessageID();
     }
     else if ("JBMPriority".equals(fieldName))
     {
        return new Integer(msg.getPriority());
     }
     else if ("JBMTimestamp".equals(fieldName))
     {
        return msg.getTimestamp();
     }
     else if ("JBMDurable".equals(fieldName))
     {
        return msg.isDurable() ? "DURABLE" : "NON_DURABLE";
     }
     else if ("JBMExpiration".equals(fieldName))
     {
        return msg.getExpiration();
     }
     else
     {
        return null;
     }     
  }
}
