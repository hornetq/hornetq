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

package org.jboss.messaging.core.management.impl;

import static org.jboss.messaging.core.security.impl.SecurityStoreImpl.CLUSTER_ADMIN_USER;

import javax.management.ObjectName;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ReplicationOperationInvoker;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.util.SimpleString;

/**
 * A ReplicationOperationInvoker
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ReplicationOperationInvokerImpl implements ReplicationOperationInvoker
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicationOperationInvokerImpl.class);

   // Attributes ----------------------------------------------------

   private final long timeout;

   private final String clusterPassword;

   private final SimpleString managementAddress;

   private ClientSession clientSession;

   private ClientRequestor requestor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationOperationInvokerImpl(final String clusterPassword,
                                         final SimpleString managementAddress,
                                         final long managementRequestTimeout)
   {
      this.timeout = managementRequestTimeout;
      this.clusterPassword = clusterPassword;
      this.managementAddress = managementAddress;
   }

   // Public --------------------------------------------------------

   public synchronized Object invoke(final ObjectName objectName,
                                           final String operationName,
                                           final Object... parameters) throws Exception
   {
      if (clientSession == null)
      {
         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
         clientSession = sf.createSession(CLUSTER_ADMIN_USER, clusterPassword, false, true, true, false, 1);
         requestor = new ClientRequestor(clientSession, managementAddress);
         clientSession.start();
      }
      ClientMessage mngmntMessage = clientSession.createClientMessage(false);
      ManagementHelper.putOperationInvocation(mngmntMessage, objectName, operationName, parameters);
      ClientMessage reply = requestor.request(mngmntMessage, timeout);

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
   
   public void stop() 
   {
      if (requestor != null && !clientSession.isClosed())
      {
         try
         {
            {
               requestor.close();
            }
         }
         catch (Exception e)
         {
            // this will happen if the remoting service is stopped before this method is called
            log.warn("Got Exception while closing requestor", e);
         }
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
