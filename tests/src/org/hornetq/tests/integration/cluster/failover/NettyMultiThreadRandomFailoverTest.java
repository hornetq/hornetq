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

package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQ;
import org.hornetq.integration.transports.netty.TransportConstants;

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
                .add(new TransportConfiguration("org.hornetq.integration.transports.netty.NettyAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupServer = HornetQ.newHornetQServer(backupConf, false);
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
      backupConf.setJMXManagementEnabled(false);
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations().clear();
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.integration.transports.netty.NettyAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveServer = HornetQ.newHornetQServer(liveConf, false);
      liveServer.start();
   }

   @Override
   protected ClientSessionFactoryInternal createSessionFactory()
   {
      final ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory"),
                                                                           new TransportConfiguration("org.hornetq.integration.transports.netty.NettyConnectorFactory",
                                                                                                      backupParams));

      sf.setProducerWindowSize(32 * 1024);
      return sf;
   }

}
