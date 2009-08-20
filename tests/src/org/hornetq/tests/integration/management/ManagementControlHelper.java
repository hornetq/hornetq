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

package org.hornetq.tests.integration.management;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import org.hornetq.core.management.AcceptorControl;
import org.hornetq.core.management.AddressControl;
import org.hornetq.core.management.BridgeControl;
import org.hornetq.core.management.BroadcastGroupControl;
import org.hornetq.core.management.ClusterConnectionControl;
import org.hornetq.core.management.DiscoveryGroupControl;
import org.hornetq.core.management.DivertControl;
import org.hornetq.core.management.HornetQServerControl;
import org.hornetq.core.management.ObjectNames;
import org.hornetq.core.management.QueueControl;
import org.hornetq.jms.server.management.ConnectionFactoryControl;
import org.hornetq.jms.server.management.JMSQueueControl;
import org.hornetq.jms.server.management.JMSServerControl;
import org.hornetq.jms.server.management.TopicControl;
import org.hornetq.utils.SimpleString;

/**
 * A ManagementControlHelper
 *
 * @author jmesnil
 *
 */
public class ManagementControlHelper
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static AcceptorControl createAcceptorControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (AcceptorControl)createProxy(ObjectNames.getAcceptorObjectName(name),
                                               AcceptorControl.class,
                                               mbeanServer);
   }

   public static BroadcastGroupControl createBroadcastGroupControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (BroadcastGroupControl)createProxy(ObjectNames.getBroadcastGroupObjectName(name),
                                                     BroadcastGroupControl.class,
                                                     mbeanServer);
   }

   public static DiscoveryGroupControl createDiscoveryGroupControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (DiscoveryGroupControl)createProxy(ObjectNames.getDiscoveryGroupObjectName(name),
                                                     DiscoveryGroupControl.class,
                                                     mbeanServer);
   }

   public static BridgeControl createBridgeControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (BridgeControl)createProxy(ObjectNames.getBridgeObjectName(name),
                                                  BridgeControl.class,
                                                  mbeanServer);
   }

   public static DivertControl createDivertControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (DivertControl)createProxy(ObjectNames.getDivertObjectName(new SimpleString(name)),
                                             DivertControl.class,
                                             mbeanServer);
   }

   public static ClusterConnectionControl createClusterConnectionControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (ClusterConnectionControl)createProxy(ObjectNames.getClusterConnectionObjectName(name),
                                                        ClusterConnectionControl.class,
                                                        mbeanServer);
   }

   public static HornetQServerControl createMessagingServerControl(MBeanServer mbeanServer) throws Exception
   {
      return (HornetQServerControl)createProxy(ObjectNames.getMessagingServerObjectName(),
                                                      HornetQServerControl.class,
                                                      mbeanServer);
   }

   public static QueueControl createQueueControl(SimpleString address, SimpleString name, MBeanServer mbeanServer) throws Exception
   {
      return (QueueControl)createProxy(ObjectNames.getQueueObjectName(address, name),
                                            QueueControl.class,
                                            mbeanServer);
   }

   public static AddressControl createAddressControl(SimpleString address, MBeanServer mbeanServer) throws Exception
   {
      return (AddressControl)createProxy(ObjectNames.getAddressObjectName(address),
                                              AddressControl.class,
                                              mbeanServer);
   }

   public static JMSQueueControl createJMSQueueControl(Queue queue, MBeanServer mbeanServer) throws Exception
   {
      return createJMSQueueControl(queue.getQueueName(), mbeanServer);
   }

   public static JMSQueueControl createJMSQueueControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (JMSQueueControl)createProxy(ObjectNames.getJMSQueueObjectName(name),
                                               JMSQueueControl.class,
                                               mbeanServer);
   }

   public static JMSServerControl createJMSServerControl(MBeanServer mbeanServer) throws Exception
   {
      return (JMSServerControl)createProxy(ObjectNames.getJMSServerObjectName(),
                                                JMSServerControl.class,
                                                mbeanServer);
   }

   public static ConnectionFactoryControl createConnectionFactoryControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return (ConnectionFactoryControl)createProxy(ObjectNames.getConnectionFactoryObjectName(name),
                                                        ConnectionFactoryControl.class,
                                                        mbeanServer);
   }

   public static TopicControl createTopicControl(Topic topic, MBeanServer mbeanServer) throws Exception
   {
      return (TopicControl)createProxy(ObjectNames.getJMSTopicObjectName(topic.getTopicName()),
                                            TopicControl.class,
                                            mbeanServer);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static Object createProxy(ObjectName objectName, Class mbeanInterface, MBeanServer mbeanServer)
   {
      return MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName, mbeanInterface, false);
   }

   // Inner classes -------------------------------------------------

}
