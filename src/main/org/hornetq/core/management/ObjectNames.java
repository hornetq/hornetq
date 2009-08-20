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

package org.hornetq.core.management;

import static java.lang.String.format;
import static javax.management.ObjectName.getInstance;
import static javax.management.ObjectName.quote;

import javax.management.ObjectName;

import org.hornetq.utils.SimpleString;

/**
 * A ObjectNames
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ObjectNames
{

   // Constants -----------------------------------------------------

   public static final String DOMAIN = "org.hornetq";

   public static final String JMS_MODULE = "JMS";

   public static final String CORE_MODULE = "Core";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ObjectName getHornetQServerObjectName() throws Exception
   {
      return getInstance(DOMAIN + ":module=Core,type=Server");
   }

   public static ObjectName getAddressObjectName(final SimpleString address) throws Exception
   {
      return createObjectName(CORE_MODULE, "Address", address.toString());
   }

   public static ObjectName getQueueObjectName(final SimpleString address, final SimpleString name) throws Exception
   {
      return getInstance(format("%s:module=%s,type=%s,address=%s,name=%s",
                                DOMAIN,
                                CORE_MODULE,
                                "Queue",
                                quote(address.toString()),
                                quote(name.toString())));
   }

   public static ObjectName getDivertObjectName(final SimpleString name) throws Exception
   {
      return createObjectName(CORE_MODULE, "Divert", name.toString());
   }

   public static ObjectName getAcceptorObjectName(final String name) throws Exception
   {
      return createObjectName(CORE_MODULE, "Acceptor", name);
   }

   public static ObjectName getBroadcastGroupObjectName(final String name) throws Exception
   {
      return createObjectName(CORE_MODULE, "BroadcastGroup", name);
   }

   public static ObjectName getBridgeObjectName(final String name) throws Exception
   {
      return createObjectName(CORE_MODULE, "JMSBridge", name);
   }
   
   public static ObjectName getClusterConnectionObjectName(String name) throws Exception
   {
      return createObjectName(CORE_MODULE, "ClusterConnection", name);
   }

   public static ObjectName getDiscoveryGroupObjectName(final String name) throws Exception
   {
      return createObjectName(CORE_MODULE, "DiscoveryGroup", name);
   }

   public static ObjectName getJMSServerObjectName() throws Exception
   {
      return getInstance(DOMAIN + ":module=JMS,type=Server");
   }

   public static ObjectName getJMSQueueObjectName(final String name) throws Exception
   {
      return createObjectName(JMS_MODULE, "Queue", name);
   }

   public static ObjectName getJMSTopicObjectName(final String name) throws Exception
   {
      return createObjectName(JMS_MODULE, "Topic", name);
   }

   public static ObjectName getConnectionFactoryObjectName(final String name) throws Exception
   {
      return createObjectName(JMS_MODULE, "ConnectionFactory", name);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static ObjectName createObjectName(final String module, final String type, final String name) throws Exception
   {
      return getInstance(format("%s:module=%s,type=%s,name=%s", DOMAIN, module, type, quote(name)));
   }

   // Inner classes -------------------------------------------------

   
   
}
