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

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import javax.management.ObjectName;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;

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

   public Object retrieveAttributeValue(String attributeName)
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putAttribute(m, on, attributeName);
      ClientMessage reply;
      try
      {
         reply = requestor.request(m);
         return ManagementHelper.getResult(reply);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object invokeOperation(String operationName, Object... args) throws Exception
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putOperationInvocation(m, on, operationName, args);
      ClientMessage reply = requestor.request(m);
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return ManagementHelper.getResult(reply);
      }
      else
      {
         throw new Exception(ManagementHelper.getOperationExceptionMessage(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
