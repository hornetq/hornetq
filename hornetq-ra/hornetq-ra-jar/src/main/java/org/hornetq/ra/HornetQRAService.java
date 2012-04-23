/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.ra;

import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;


/**
 * A HornetQRAService ensures that HornetQ Resource Adapter will be stopped *before* the HornetQ server.
 * https://jira.jboss.org/browse/HORNETQ-339
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class HornetQRAService
{
   // Constants -----------------------------------------------------
   // Attributes ----------------------------------------------------

   private final MBeanServer mBeanServer;

   private final String resourceAdapterObjectName;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public HornetQRAService(final MBeanServer mBeanServer, final String resourceAdapterObjectName)
   {
      this.mBeanServer = mBeanServer;
      this.resourceAdapterObjectName = resourceAdapterObjectName;
   }

   // Public --------------------------------------------------------

   public void stop()
   {
      try
      {
         ObjectName objectName = new ObjectName(resourceAdapterObjectName);
         Set<ObjectInstance> mbeanSet = mBeanServer.queryMBeans(objectName, null);

         for (ObjectInstance mbean : mbeanSet)
         {
            String stateString = (String)mBeanServer.getAttribute(mbean.getObjectName(), "StateString");

            if ("Started".equalsIgnoreCase(stateString) || "Starting".equalsIgnoreCase(stateString))
            {
               mBeanServer.invoke(mbean.getObjectName(), "stop", new Object[0], new String[0]);
            }
         }
      }
      catch (Exception e)
      {
         HornetQRALogger.LOGGER.errorStoppingRA(e);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
