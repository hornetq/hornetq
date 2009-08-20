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

package org.hornetq.core.management.impl;

import java.util.HashMap;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientRequestor;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ReplicationOperationInvoker;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.utils.SimpleString;

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

   private final String clusterUser;

   private final String clusterPassword;

   private final SimpleString managementAddress;

   private ClientSession clientSession;

   private ClientRequestor requestor;

   private final int managementConnectorID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationOperationInvokerImpl(final String clusterUser,
                                          final String clusterPassword,
                                          final SimpleString managementAddress,
                                          final long managementRequestTimeout,
                                          final int managementConnectorID)
   {
      this.timeout = managementRequestTimeout;
      this.clusterUser = clusterUser;
      this.clusterPassword = clusterPassword;
      this.managementAddress = managementAddress;
      this.managementConnectorID = managementConnectorID;
   }

   // Public --------------------------------------------------------

   public synchronized Object invoke(final String resourceName, final String operationName, final Object... parameters) throws Exception
   {
      if (clientSession == null)
      {
         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                           new HashMap<String, Object>()
                                                                                           {
                                                                                              {
                                                                                                 put(TransportConstants.SERVER_ID_PROP_NAME,
                                                                                                     managementConnectorID);
                                                                                              }
                                                                                           }));

         clientSession = sf.createSession(clusterUser, clusterPassword, false, true, true, false, 1);
         requestor = new ClientRequestor(clientSession, managementAddress);
         clientSession.start();
      }
      ClientMessage mngmntMessage = clientSession.createClientMessage(false);
      ManagementHelper.putOperationInvocation(mngmntMessage, resourceName, operationName, parameters);
      ClientMessage reply = requestor.request(mngmntMessage, timeout);

      if (reply == null)
      {
         throw new Exception("did not receive reply for message " + mngmntMessage);
      }
      reply.acknowledge();
      if (ManagementHelper.hasOperationSucceeded(reply))
      {
         return ManagementHelper.getResult(reply);
      }
      else
      {
         throw new Exception((String)ManagementHelper.getResult(reply));
      }
   }

   public void stop() throws Exception
   {
      if (clientSession != null)
      {
         clientSession.close();
      }
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
