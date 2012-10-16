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

package org.hornetq.api.jms.management;

import javax.jms.JMSException;
import javax.jms.Message;

import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.jms.client.HornetQMessage;

/**
 * Helper class to use JMS messages to manage HornetQ server resources.
 *
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

   private static org.hornetq.api.core.Message getCoreMessage(final Message jmsMessage)
   {
      if (jmsMessage instanceof HornetQMessage == false)
      {
         throw new IllegalArgumentException("Cannot send a non HornetQ message as a management message " + jmsMessage.getClass()
                                                                                                                   .getName());
      }

      return ((HornetQMessage)jmsMessage).getCoreMessage();
   }

   /**
    * Stores a resource attribute in a JMS message to retrieve the value from the server resource.
    *
    * @param message JMS message
    * @param resourceName the name of the resource
    * @param attribute the name of the attribute
    * @throws JMSException if an exception occurs while putting the information in the message
    *
    * @see ResourceNames
    */
   public static void putAttribute(final Message message, final String resourceName, final String attribute) throws JMSException
   {
      ManagementHelper.putAttribute(JMSManagementHelper.getCoreMessage(message), resourceName, attribute);
   }

   /**
    * Stores a operation invocation in a JMS message to invoke the corresponding operation the value from the server resource.
    *
    * @param message JMS message
    * @param resourceName the name of the resource
    * @param operationName the name of the operation to invoke on the resource
    * @throws JMSException if an exception occurs while putting the information in the message
    *
    * @see ResourceNames
    */
   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName) throws JMSException
   {
      try
      {
         ManagementHelper.putOperationInvocation(JMSManagementHelper.getCoreMessage(message),
                                                 resourceName,
                                                 operationName);
      }
      catch (Exception e)
      {
         throw JMSManagementHelper.convertFromException(e);
      }
   }

   private static JMSException convertFromException(final Exception e)
   {
      JMSException jmse = new JMSException(e.getMessage());

      jmse.initCause(e);

      return jmse;
   }

   /**
    * Stores a operation invocation in a JMS message to invoke the corresponding operation the value from the server resource.
    *
    * @param message JMS message
    * @param resourceName the name of the server resource
    * @param operationName the name of the operation to invoke on the server resource
    * @param parameters the parameters to use to invoke the server resource
    * @throws JMSException if an exception occurs while putting the information in the message
    *
    * @see ResourceNames
    */
   public static void putOperationInvocation(final Message message,
                                             final String resourceName,
                                             final String operationName,
                                             final Object... parameters) throws JMSException
   {
      try
      {
         ManagementHelper.putOperationInvocation(JMSManagementHelper.getCoreMessage(message),
                                                 resourceName,
                                                 operationName,
                                                 parameters);
      }
      catch (Exception e)
      {
         throw JMSManagementHelper.convertFromException(e);
      }
   }

   /**
    * Returns whether the JMS message corresponds to the result of a management operation invocation.
    */
   public static boolean isOperationResult(final Message message) throws JMSException
   {
      return ManagementHelper.isOperationResult(JMSManagementHelper.getCoreMessage(message));
   }

   /**
    * Returns whether the JMS message corresponds to the result of a management attribute value.
    */
   public static boolean isAttributesResult(final Message message) throws JMSException
   {
      return ManagementHelper.isAttributesResult(JMSManagementHelper.getCoreMessage(message));
   }

   /**
    * Returns whether the invocation of the management operation on the server resource succeeded.
    */
   public static boolean hasOperationSucceeded(final Message message) throws JMSException
   {
      return ManagementHelper.hasOperationSucceeded(JMSManagementHelper.getCoreMessage(message));
   }

   /**
    * Returns the result of an operation invocation or an attribute value.
    * <br>
    * If an error occurred on the server, {@link #hasOperationSucceeded(Message)} will return {@code false}.
    * and the result will be a String corresponding to the server exception.
    */
   public static Object[] getResults(final Message message) throws Exception
   {
      return ManagementHelper.getResults(JMSManagementHelper.getCoreMessage(message));
   }

   /**
    * Returns the result of an operation invocation or an attribute value.
    * <br>
    * If an error occurred on the server, {@link #hasOperationSucceeded(Message)} will return {@code false}.
    * and the result will be a String corresponding to the server exception.
    */
   public static Object getResult(final Message message) throws Exception
   {
      return ManagementHelper.getResult(JMSManagementHelper.getCoreMessage(message));
   }

   // Constructors --------------------------------------------------

   private JMSManagementHelper()
   {
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
