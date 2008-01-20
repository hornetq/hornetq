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
package org.jboss.messaging.core.impl.filter;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Message;

/**
* This class implements a JBoss Messaging filter
* 
* @author     <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
* 
* Derived from JBoss MQ version by
* 
* @author <a href="mailto:Norbert.Lataille@m4x.org">Norbert Lataille</a>
* @author <a href="mailto:jplindfo@helsinki.fi">Juha Lindfors</a>
* @author <a href="mailto:jason@planet57.com">Jason Dillon</a>
* @author <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
* @authro <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
* 
* @version    $Revision: 3569 $
*
* $Id: Selector.java 3569 2008-01-15 21:14:04Z timfox $
*/
public class FilterImpl implements Filter
{
  private static final Logger log = Logger.getLogger(FilterImpl.class);
  
  private String filterString;

  private Map<String, Identifier> identifiers = new HashMap<String, Identifier>();
  
  private Operator operator;
  
  private FilterParser parser = new FilterParser();
  
  public FilterImpl(String filterString) throws Exception
  {
     this.filterString = filterString;

     try
     {
        operator = (Operator)parser.parse(filterString, identifiers);
     }
     catch (Throwable e)
     {
        throw new IllegalArgumentException("Invalid filter: " + filterString);
     }
  }
  
  // Filter implementation ---------------------------------------------------------------------
  
  public String getFilterString()
  {
     return filterString;
  }
  
  public boolean match(Message message)
  {
     try
     {                 
        // Set the identifiers values
                 
        for (Identifier id : identifiers.values())
        { 
           Object val = null;
           
           if (id.name.startsWith("JBM"))
           {           
              //Look it up as header fields
              
              val = getHeaderFieldValue(message, id.name);
           }
                     
           if (val == null)
           {
              //First look it up in the headers
              
              val = message.getHeader(id.name);                            
           }
           
           if (val != null)
           {
              id.value = val;
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
 
  private Object getHeaderFieldValue(Message msg, String fieldName)
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
