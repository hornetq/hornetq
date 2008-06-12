/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.remoting.impl.invm;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Acceptor;
import org.jboss.messaging.core.remoting.CleanUpNotifier;
import org.jboss.messaging.core.remoting.ConnectorRegistryFactory;
import org.jboss.messaging.core.remoting.RemotingService;

/**
 * An INVM Acceptor. This will allow connections from within the same VM via the ConnectorRegistry
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
//todo this will work in conjunction with the INVMConnector, packets will be serialised instead of passed by reference directly to the dispatcher 
public class INVMAcceptor implements Acceptor
{
   private static final Logger log = Logger.getLogger(INVMAcceptor.class);

   private RemotingService remotingService;

   public void startAccepting(RemotingService remotingService, CleanUpNotifier cleanupNotifier) throws Exception
   {
      this.remotingService = remotingService;
      log.info("Registering INVMAcceptor with location:" + remotingService.getConfiguration().getLocation());
      ConnectorRegistryFactory.getRegistry().register(remotingService.getConfiguration().getLocation(), remotingService.getDispatcher());
   }

   public void stopAccepting()
   {
      ConnectorRegistryFactory.getRegistry().unregister(remotingService.getConfiguration().getLocation());
   }
}
