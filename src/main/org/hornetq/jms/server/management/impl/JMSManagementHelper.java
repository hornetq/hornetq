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

package org.hornetq.jms.server.management.impl;

import javax.jms.JMSException;
import javax.jms.Message;

import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.jms.client.HornetQMessage;

/*
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class JMSManagementHelper
{
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   private static org.hornetq.core.message.Message getCoreMessage(final Message jmsMessage)
   {
      if (jmsMessage instanceof HornetQMessage == false)
      {
         throw new IllegalArgumentException("Cannot send a non JBoss message as a management message " +
                                            jmsMessage.getClass().getName());
      }
      
      return ((HornetQMessage)jmsMessage).getCoreMessage();
   }
   
   public static void putAttribute(final Message message, final String resourceName, final String attribute) throws JMSException
   {
      ManagementHelper.putAttribute(getCoreMessage(message), resourceName, attribute);
   }
   
   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName) throws JMSException
   {      
      try
      {
         ManagementHelper.putOperationInvocation(getCoreMessage(message), resourceName, operationName);
      }
      catch (Exception e)
      {
         throw convertFromException(e);
      }
   }
   
   private static JMSException convertFromException(Exception e)
   {
      JMSException jmse =  new JMSException(e.getMessage());
      
      jmse.initCause(e);
      
      return jmse;
   }

   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName,
                                             final Object... parameters) throws JMSException
   {
      try
      {
         ManagementHelper.putOperationInvocation(getCoreMessage(message), resourceName, operationName, parameters);
      }
      catch (Exception e)
      {
         throw convertFromException(e);
      }
   }

   public static boolean isOperationResult(final Message message) throws JMSException
   {
      return ManagementHelper.isOperationResult(getCoreMessage(message));
   }

   public static boolean isAttributesResult(final Message message) throws JMSException
   {
      return ManagementHelper.isAttributesResult(getCoreMessage(message));
   }

   public static boolean hasOperationSucceeded(final Message message) throws JMSException
   {
      return ManagementHelper.hasOperationSucceeded(getCoreMessage(message));
   }
   
   public static Object[] getResults(final Message message) throws Exception
   {
      return ManagementHelper.getResults(getCoreMessage(message));
   }

   public static Object getResult(final Message message) throws Exception
   {
      return ManagementHelper.getResult(getCoreMessage(message));
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
