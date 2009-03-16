/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.management.core;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.utils.SimpleString;

/**
 * A MBeanUsingCoreMessage
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class CoreMessagingProxy
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ObjectName on;

   private ClientSession session;

   private ClientRequestor requestor;

   // Static --------------------------------------------------------

   protected static String fromNullableSimpleString(SimpleString sstring)
   {
      if (sstring == null)
      {
         return null;
      }
      else
      {
         return sstring.toString();
      }
   }

   // Constructors --------------------------------------------------

   public CoreMessagingProxy(ClientSession session, ObjectName objectName) throws Exception
   {
      this.session = session;

      this.on = objectName;

      this.requestor = new ClientRequestor(session, DEFAULT_MANAGEMENT_ADDRESS);

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Object retriveAttributeValue(String attributeName)
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putAttributes(m, on, attributeName);
      ClientMessage reply;
      try
      {
         reply = requestor.request(m);
         Object attributeValue = reply.getProperty(new SimpleString(attributeName));
         if (attributeValue.equals(new SimpleString("null")))
         {
            return null;
         }
         return attributeValue;
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object[] retrieveArrayAttribute(String attributeName)
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putAttributes(m, on, attributeName);
      try
      {
         ClientMessage reply = requestor.request(m);
         return (Object[])ManagementHelper.getArrayProperty(reply, attributeName);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }
   
   public TabularData retrieveTabularAttribute(String attributeName)
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putAttributes(m, on, attributeName);
      try
      {
         ClientMessage reply = requestor.request(m);
         return (TabularData)ManagementHelper.getTabularDataProperty(reply, attributeName);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   protected Object invokOperation(String operationName, Object... args) throws Exception
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putOperationInvocation(m, on, operationName, args);
      ClientMessage reply = requestor.request(m);
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return reply.getProperty(new SimpleString(operationName));
      }
      else
      {
         throw new Exception(ManagementHelper.getOperationExceptionMessage(reply));
      }
   }

   protected TabularData invokeTabularOperation(String operationName, Object... args) throws Exception
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putOperationInvocation(m, on, operationName, args);
      ClientMessage reply = requestor.request(m);
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return ManagementHelper.getTabularDataProperty(reply, operationName);
      }
      else
      {
         throw new Exception(ManagementHelper.getOperationExceptionMessage(reply));
      }
   }

   protected CompositeData invokeCompositeOperation(String operationName, Object... args) throws Exception
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putOperationInvocation(m, on, operationName, args);
      ClientMessage reply = requestor.request(m);
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return ManagementHelper.getCompositeDataProperty(reply, operationName);
      }
      else
      {
         throw new Exception(ManagementHelper.getOperationExceptionMessage(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
