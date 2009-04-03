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

package org.jboss.messaging.core.management;

import static java.lang.String.format;
import static javax.management.ObjectName.getInstance;
import static javax.management.ObjectName.quote;

import javax.management.ObjectName;

import org.jboss.messaging.utils.SimpleString;

/**
 * A ObjectNames
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class ObjectNames
{

   // Constants -----------------------------------------------------

   public static final String DOMAIN = "org.jboss.messaging";

   public static final String JMS_MODULE = "JMS";

   public static final String CORE_MODULE = "Core";

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static ObjectName getMessagingServerObjectName() throws Exception
   {
      return getInstance(DOMAIN + ":module=Core,type=Server");
   }

   public static ObjectName getResourceManagerObjectName() throws Exception
   {
      return getInstance(DOMAIN + ":module=Core,type=ResourceManager");
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
