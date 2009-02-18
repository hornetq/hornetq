/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.management.jmx.impl;

import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.jboss.messaging.core.management.ReplicationOperationInvoker;

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

   // Attributes ----------------------------------------------------

   private final ObjectName objectName;

   private final ReplicationOperationInvoker replicationInvoker;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected ReplicationAwareStandardMBeanWrapper(final ObjectName objectName,
                                                  final Class mbeanInterface,
                                                  final ReplicationOperationInvoker replicationInvoker) throws Exception
   {
      super(mbeanInterface);

      this.objectName = objectName;
      this.replicationInvoker = replicationInvoker;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Object replicationAwareInvoke(final String operationName, final Object... parameters) throws Exception
   {
      return replicationInvoker.invoke(objectName, operationName, parameters);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
