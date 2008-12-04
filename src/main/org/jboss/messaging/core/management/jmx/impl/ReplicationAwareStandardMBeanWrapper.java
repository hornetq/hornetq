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

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * A ReplicationAwareStandardMBeanWrapper
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 3 déc. 2008 11:23:11
 *
 *
 */
public class ReplicationAwareStandardMBeanWrapper extends StandardMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ObjectName objectName;

   private final ClientSessionFactoryImpl sessionFactory;

   // FIXME moved to configuration
   private final long timeout = 500;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   protected ReplicationAwareStandardMBeanWrapper(final ObjectName objectName, final Class mbeanInterface) throws NotCompliantMBeanException
   {
      super(mbeanInterface);

      this.objectName = objectName;
      this.sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected Object replicationAwareInvoke(final String operationName, final Object... parameters) throws Exception
   {
      ClientSession clientSession = sessionFactory.createSession(false, true, true);
      ClientRequestor requestor = new ClientRequestor(clientSession, ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS);
      clientSession.start();

      ClientMessage mngmntMessage = clientSession.createClientMessage(false);
      ManagementHelper.putOperationInvocation(mngmntMessage, objectName, operationName, parameters);
      ClientMessage reply = requestor.request(mngmntMessage, timeout);

      try
      {
         if (reply == null)
         {
            throw new Exception("did not receive reply for message " + mngmntMessage);
         }
         if (ManagementHelper.hasOperationSucceeded(reply))
         {
            return reply.getProperty(new SimpleString(operationName));
         }
         else
         {
            throw new Exception(ManagementHelper.getOperationExceptionMessage(reply));
         }
      }
      finally
      {
         requestor.close();
      }
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
