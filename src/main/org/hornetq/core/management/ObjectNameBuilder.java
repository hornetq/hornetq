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

import javax.management.ObjectName;

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.utils.SimpleString;

/**
 * A ObjectNameBuilder
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ObjectNameBuilder
{

   // Constants -----------------------------------------------------

   public static ObjectNameBuilder DEFAULT = new ObjectNameBuilder(ConfigurationImpl.DEFAULT_JMX_DOMAIN);

   public static final String JMS_MODULE = "JMS";

   public static final String CORE_MODULE = "Core";

   // Attributes ----------------------------------------------------

   private final String domain;

   // Static --------------------------------------------------------

   public static ObjectNameBuilder create(final String domain)
   {
      return new ObjectNameBuilder(domain);
   }

   // Constructors --------------------------------------------------

   private ObjectNameBuilder(final String domain)
   {
      this.domain = domain;
   }

   // Public --------------------------------------------------------

   public ObjectName getHornetQServerObjectName() throws Exception
   {
      return ObjectName.getInstance(domain + ":module=Core,type=Server");
   }

   public ObjectName getAddressObjectName(final SimpleString address) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Address", address.toString());
   }

   public ObjectName getQueueObjectName(final SimpleString address, final SimpleString name) throws Exception
   {
      return ObjectName.getInstance(String.format("%s:module=%s,type=%s,address=%s,name=%s",
                                                  domain,
                                                  ObjectNameBuilder.CORE_MODULE,
                                                  "Queue",
                                                  ObjectName.quote(address.toString()),
                                                  ObjectName.quote(name.toString())));
   }

   public ObjectName getDivertObjectName(final SimpleString name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Divert", name.toString());
   }

   public ObjectName getAcceptorObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "Acceptor", name);
   }

   public ObjectName getBroadcastGroupObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "BroadcastGroup", name);
   }

   public ObjectName getBridgeObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "JMSBridge", name);
   }

   public ObjectName getClusterConnectionObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "ClusterConnection", name);
   }

   public ObjectName getDiscoveryGroupObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.CORE_MODULE, "DiscoveryGroup", name);
   }

   public ObjectName getJMSServerObjectName() throws Exception
   {
      return ObjectName.getInstance(domain + ":module=JMS,type=Server");
   }

   public ObjectName getJMSQueueObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.JMS_MODULE, "Queue", name);
   }

   public ObjectName getJMSTopicObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.JMS_MODULE, "Topic", name);
   }

   public ObjectName getConnectionFactoryObjectName(final String name) throws Exception
   {
      return createObjectName(ObjectNameBuilder.JMS_MODULE, "ConnectionFactory", name);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private ObjectName createObjectName(final String module, final String type, final String name) throws Exception
   {
      return ObjectName.getInstance(String.format("%s:module=%s,type=%s,name=%s",
                                                  domain,
                                                  module,
                                                  type,
                                                  ObjectName.quote(name)));
   }

   // Inner classes -------------------------------------------------

}
