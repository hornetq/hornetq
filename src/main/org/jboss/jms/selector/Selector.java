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
package org.jboss.jms.selector;

import java.util.HashMap;
import java.util.Iterator;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Routable;


/**
 * This class implements a Message Selector.
 *
 * @author     <a href="mailto:Norbert.Lataille@m4x.org">Norbert Lataille</a>
 * @author     <a href="mailto:jplindfo@helsinki.fi">Juha Lindfors</a>
 * @author     <a href="mailto:jason@planet57.com">Jason Dillon</a>
 * @author     <a href="mailto:Scott.Stark@jboss.org">Scott Stark</a>
 * @author	   <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version    $Revision$
 *
 * $Id$
 */
public class Selector implements Filter
{
   /** The serialVersionUID */
   private static final long serialVersionUID = 1456143116781848968L;

   /** The logging interface */
   static Logger cat = Logger.getLogger(Selector.class);
   
   /** The ISelectorParser implementation class */
   private static Class parserClass = SelectorParser.class;
   
   private static final Logger log = Logger.getLogger(Selector.class);
   
   public String selector;

   public HashMap identifiers;
   
   public Object result;
   
   private Class resultType;

   /**
    * Get the class that implements the ISelectorParser interface to be used by
    * Selector instances.
    */
   public static Class getSelectorParserClass()
   {
      return Selector.parserClass;
   }
   
   /**
    * Set the class that implements the ISelectorParser interface to be used by
    * Selector instances.
    * 
    * @param parserClass  the ISelectorParser implementation. This must have a
    *                     public no-arg constructor.
    */
   public static void setSelectorParserClass(Class parserClass)
   {
      Selector.parserClass = parserClass;
   }

   public Selector(String sel) throws InvalidSelectorException
   {
      selector = sel;
      identifiers = new HashMap();
      
      try
      {
         ISelectorParser bob = (ISelectorParser) parserClass.newInstance();
         result = bob.parse(sel, identifiers);
         resultType = result.getClass();
      }
      catch (Exception e)
      {
         if (log.isTraceEnabled()) { log.trace("Invalid selector:" + sel); }

         InvalidSelectorException exception =
            new InvalidSelectorException("The selector is invalid: " + sel);         
         throw exception;
      }
   }
   
   public String getExpression()
   {
      return selector;
   }
	
	public synchronized boolean accept(Routable routable)
   {
      try
      {			
         org.jboss.messaging.core.Message m;
         
         //FIXME - We can get rid of this nasty comparisons once we change core
         //to only deal with MessageRefernce instances
         if (routable.isReference())
         {
            m = ((MessageReference)routable).getMessage();
         }
         else
         {
            m = (org.jboss.messaging.core.Message)routable;
         }
         
         //Only accept JMS messages
         if (!(m instanceof Message))
         {
            return false;
         }
         
         Message mess = (Message)m;
         			
         // Set the identifiers values
         Iterator i = identifiers.values().iterator();
         
         while (i.hasNext())
         {
            Identifier id = (Identifier) i.next();
            
            Object find = mess.getObjectProperty(id.name);
            
            if (find == null)
               find = getHeaderFieldReferences(mess, id.name);
            
            if (find == null)
               id.value = null;
            else
            {
               Class type = find.getClass();
               if (type.equals(Boolean.class) ||
                   type.equals(String.class)  ||
                   type.equals(Double.class)  ||
                   type.equals(Float.class)   ||
                   type.equals(Integer.class) ||
                   type.equals(Long.class)    ||
                   type.equals(Short.class)   ||
                   type.equals(Byte.class))
                  id.value = find;
               else
                  throw new Exception("Bad property '" + id.name + "' type: " + type);
            }
         }
         
         // Compute the result of this operator
         Object res;
         
         if (resultType.equals(Identifier.class))
            res = ((Identifier)result).value;
         else if (resultType.equals(Operator.class))
         {
            Operator op = (Operator) result;
            res = op.apply();
         }
         else
            res = result;
         
         if (res == null)
            return false;
         
         if (!(res.getClass().equals(Boolean.class)))
            throw new Exception("Bad object type: " + res);
         
         return ((Boolean) res).booleanValue();
      }
      catch (Exception e)
      {
         cat.warn("Invalid selector: " + selector, e);
         return false;
      }
   }

  
   // [JPL]
   private Object getHeaderFieldReferences(Message mess, String idName)
      throws JMSException
   {
      // JMS 3.8.1.1 -- Message header field references are restricted to:
      //                JMSDeliveryMode, JMSPriority, JMSMessageID,
      //                JMSTimeStamp, JMSCorrelationID and JMSType
      //
      if (idName.equals("JMSDeliveryMode"))
         return new Integer(mess.getJMSDeliveryMode());
      else if (idName.equals("JMSPriority"))
         return new Integer(mess.getJMSPriority());
      else if (idName.equals("JMSMessageID"))
         return mess.getJMSMessageID();
      else if (idName.equals("JMSTimestamp"))
         return new Long(mess.getJMSTimestamp());
      else if (idName.equals("JMSCorrelationID"))
         return mess.getJMSCorrelationID();
      else if (idName.equals("JMSType"))
         return mess.getJMSType();
      else
         return null;
   }
}
