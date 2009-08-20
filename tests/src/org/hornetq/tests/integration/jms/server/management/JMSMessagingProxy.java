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

package org.hornetq.tests.integration.jms.server.management;

import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.hornetq.jms.server.management.impl.JMSManagementHelper;

/**
 * A MBeanUsingCoreMessage
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class JMSMessagingProxy
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String resourceName;

   private Session session;

   private QueueRequestor requestor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public JMSMessagingProxy(QueueSession session, Queue managementQueue, String resourceName) throws Exception
   {
      this.session = session;

      this.resourceName = resourceName;

      this.requestor = new QueueRequestor(session, managementQueue);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   public Object retrieveAttributeValue(String attributeName)
   {
      try
      {
         Message m = session.createMessage();
         JMSManagementHelper.putAttribute(m, resourceName, attributeName);
         Message reply = requestor.request(m);
         return JMSManagementHelper.getResult(reply);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public Object invokeOperation(String operationName, Object... args) throws Exception
   {
      Message m = session.createMessage();
      JMSManagementHelper.putOperationInvocation(m, resourceName, operationName, args);
      Message reply = requestor.request(m);
      if (JMSManagementHelper.hasOperationSucceeded(reply))
      {
         return JMSManagementHelper.getResult(reply);
      }
      else
      {
         throw new Exception((String)JMSManagementHelper.getResult(reply));
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
