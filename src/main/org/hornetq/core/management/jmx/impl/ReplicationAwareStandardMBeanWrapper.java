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

package org.hornetq.core.management.jmx.impl;

import javax.management.StandardMBean;

import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ReplicationOperationInvoker;

/**
 * A ReplicationAwareStandardMBeanWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created Dec 3, 2008 11:23:11
 *
 *
 */
public class ReplicationAwareStandardMBeanWrapper extends StandardMBean
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(ReplicationAwareStandardMBeanWrapper.class);


   // Attributes ----------------------------------------------------

   private final String resourceName;

   private final ReplicationOperationInvoker replicationInvoker;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected ReplicationAwareStandardMBeanWrapper(final String resourceName,
                                                  final Class mbeanInterface,
                                                  final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(mbeanInterface);

      this.resourceName = resourceName;
      
      this.replicationInvoker = replicationInvoker;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Object replicationAwareInvoke(final String operationName, final Object... parameters) throws Exception
   {
      return replicationInvoker.invoke(resourceName, operationName, parameters);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
