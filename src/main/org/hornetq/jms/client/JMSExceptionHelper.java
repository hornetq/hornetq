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

package org.hornetq.jms.client;

import javax.jms.InvalidDestinationException;
import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;

import org.hornetq.core.exception.MessagingException;

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
