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

package org.jboss.messaging.jms.client;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.jboss.messaging.core.exception.MessagingException;

/**
 * 
 * A JMSExceptionHelper
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class JMSExceptionHelper
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   public static JMSException convertFromMessagingException(final MessagingException me)
   {
      JMSException je;
      switch (me.getCode())
      {
         case MessagingException.CONNECTION_TIMEDOUT:
            je = new JMSException(me.getMessage());
            break;

         case MessagingException.ILLEGAL_STATE:
            je = new javax.jms.IllegalStateException(me.getMessage());
            break;
         
         case MessagingException.INTERNAL_ERROR:
            je =  new JMSException(me.getMessage());
            break;
            
         case MessagingException.INVALID_FILTER_EXPRESSION:
            je = new InvalidSelectorException(me.getMessage());
            break;
            
         case MessagingException.NOT_CONNECTED:
            je = new JMSException(me.getMessage());
            break;
            
         case MessagingException.OBJECT_CLOSED:
            je = new javax.jms.IllegalStateException(me.getMessage());
            break;
            
         case MessagingException.QUEUE_DOES_NOT_EXIST:
            je = new InvalidDestinationException(me.getMessage());
            break;
            
         case MessagingException.QUEUE_EXISTS:
            je = new InvalidDestinationException(me.getMessage());
            break;
            
         case MessagingException.SECURITY_EXCEPTION:
            je = new JMSSecurityException(me.getMessage());
            break;
            
         case MessagingException.UNSUPPORTED_PACKET:
            je =  new javax.jms.IllegalStateException(me.getMessage());
            break;
            
         default:
            je = new JMSException(me.getMessage());
      }
      
      je.setStackTrace(me.getStackTrace());
      
      je.initCause(me);
      
      return je;
   }
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------
  
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
