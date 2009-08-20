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

package org.hornetq.tests.integration.management;

import static org.hornetq.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientRequestor;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.management.impl.ManagementHelper;

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

   private final String resourceName;

   private ClientSession session;

   private ClientRequestor requestor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public CoreMessagingProxy(ClientSession session, String resourceName) throws Exception
   {
      this.session = session;

      this.resourceName = resourceName;

      this.requestor = new ClientRequestor(session, DEFAULT_MANAGEMENT_ADDRESS);

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public Object retrieveAttributeValue(String attributeName)
   {
      return retrieveAttributeValue(attributeName, null);
   }
   
   public Object retrieveAttributeValue(String attributeName, Class desiredType)
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putAttribute(m, resourceName, attributeName);
      ClientMessage reply;
      try
      {
         reply = requestor.request(m);
         Object result = ManagementHelper.getResult(reply);
         
         if (desiredType != null && desiredType != result.getClass())
         {
            //Conversions
            if (desiredType == Long.class && result.getClass() == Integer.class)
            {
               Integer in = (Integer)result;
               
               result = new Long(in.intValue());
            }
         }
         
         return result;
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object invokeOperation(String operationName, Object... args) throws Exception
   {
      ClientMessage m = session.createClientMessage(false);
      ManagementHelper.putOperationInvocation(m, resourceName, operationName, args);
      ClientMessage reply = requestor.request(m);
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return ManagementHelper.getResult(reply);
      }
      else
      {
         throw new Exception((String)ManagementHelper.getResult(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
