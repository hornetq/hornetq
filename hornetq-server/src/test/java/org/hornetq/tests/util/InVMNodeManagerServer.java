/**
 *
 */
package org.hornetq.tests.util;

import javax.management.MBeanServer;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.spi.core.security.HornetQSecurityManager;

public final class InVMNodeManagerServer extends HornetQServerImpl
{
   final NodeManager nodeManager;

   public InVMNodeManagerServer(final NodeManager nodeManager)
   {
      super();
      this.nodeManager = nodeManager;
   }

   public InVMNodeManagerServer(final Configuration configuration, final NodeManager nodeManager)
   {
      super(configuration);
      this.nodeManager = nodeManager;
   }

   public InVMNodeManagerServer(final Configuration configuration,
                                final MBeanServer mbeanServer,
                                final NodeManager nodeManager)
   {
      super(configuration, mbeanServer);
      this.nodeManager = nodeManager;
   }

   public InVMNodeManagerServer(final Configuration configuration,
                                final HornetQSecurityManager securityManager,
                                final NodeManager nodeManager)
   {
      super(configuration, securityManager);
      this.nodeManager = nodeManager;
   }

   public InVMNodeManagerServer(final Configuration configuration,
                                final MBeanServer mbeanServer,
                                final HornetQSecurityManager securityManager,
                                final NodeManager nodeManager)
   {
      super(configuration, mbeanServer, securityManager);
      this.nodeManager = nodeManager;
   }

   @Override
   protected NodeManager createNodeManager(final String directory, final String nodeGroupName, boolean replicatingBackup)
   {
      nodeManager.setNodeGroupName(nodeGroupName);
      return nodeManager;
   }

}
