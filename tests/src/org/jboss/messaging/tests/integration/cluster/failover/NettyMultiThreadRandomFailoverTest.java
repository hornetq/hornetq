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

package org.jboss.messaging.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.integration.transports.netty.TransportConstants;

/**
 * A NettyMultiThreadRandomFailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Feb 2009 08:01:20
 *
 *
 */
public class NettyMultiThreadRandomFailoverTest extends MultiThreadRandomFailoverTest
{
   @Override
   protected void start() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setJMXManagementEnabled(false);
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT + 1);
      backupConf.getAcceptorConfigurations().clear();
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupServer = Messaging.newNullStorageMessagingServer(backupConf);
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
      backupConf.setJMXManagementEnabled(false);
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations().clear();
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveServer = Messaging.newNullStorageMessagingServer(liveConf);
      liveServer.start();
   }

   @Override
   protected ClientSessionFactoryInternal createSessionFactory()
   {
      final ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory"),
                                                                           new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory",
                                                                                                      backupParams));

      sf.setSendWindowSize(32 * 1024);
      return sf;
   }

}
