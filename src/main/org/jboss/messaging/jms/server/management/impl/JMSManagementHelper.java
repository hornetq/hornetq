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

package org.jboss.messaging.jms.server.management.impl;

import javax.jms.JMSException;
import javax.jms.Message;

import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.utils.json.JSONArray;

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

   private static org.jboss.messaging.core.message.Message getCoreMessage(final Message jmsMessage)
   {
      if (jmsMessage instanceof JBossMessage == false)
      {
         throw new IllegalArgumentException("Cannot send a non JBoss message as a management message " +
                                            jmsMessage.getClass().getName());
      }
      
      return ((JBossMessage)jmsMessage).getCoreMessage();
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
