/*
 * Copyright 2010 Red Hat, Inc.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.security.Role;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * A SecurityFailoverTest
 *
 * @author clebertsuconic
 *
 *
 */
public class SecurityFailoverTest extends FailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean isXA,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception
   {
      return sf.createSession("a",
                              "b",
                              isXA,
                              autoCommitSends,
                              autoCommitAcks,
                              sf.getServerLocator().isPreAcknowledge(),
                              ackBatchSize);
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception
   {
      return sf.createSession("a", "b", false, autoCommitSends, autoCommitAcks, sf.getServerLocator()
                                                                                  .isPreAcknowledge(), ackBatchSize);
   }

   protected ClientSession createSession(ClientSessionFactory sf, boolean autoCommitSends, boolean autoCommitAcks) throws Exception
   {
      return createSession(sf, autoCommitSends, autoCommitAcks, sf.getServerLocator().getAckBatchSize());
   }

   protected ClientSession createSession(ClientSessionFactory sf) throws Exception
   {
      return createSession(sf, true, true, sf.getServerLocator().getAckBatchSize());
   }

   protected ClientSession createSession(ClientSessionFactory sf,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception
   {
      return createSession(sf, xa, autoCommitSends, autoCommitAcks, sf.getServerLocator().getAckBatchSize());
   }

   /**
    * @throws Exception
    */
   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager();

      backupConfig = super.createDefaultConfig();
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      backupConfig.setSecurityEnabled(true);
      backupConfig.setSharedStore(true);
      backupConfig.setBackup(true);
      backupConfig.setClustered(true);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(liveConnector.getName());
      ClusterConnectionConfiguration cccLive = new ClusterConnectionConfiguration("cluster1",
                                                                                  "jms",
                                                                                  backupConnector.getName(),
                                                                                  -1,
                                                                                  false,
                                                                                  false,
                                                                                  1,
                                                                                  1,
                                                                                  staticConnectors,
                                                                                  false);
      backupConfig.getClusterConfigurations().add(cccLive);
      backupServer = createBackupServer();

      HornetQSecurityManager securityManager = installSecurity(backupServer);

      securityManager.setDefaultUser(null);

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setSecurityEnabled(true);
      liveConfig.setSharedStore(true);
      liveConfig.setClustered(true);
      List<String> pairs = null;
      ClusterConnectionConfiguration ccc0 = new ClusterConnectionConfiguration("cluster1",
                                                                               "jms",
                                                                               liveConnector.getName(),
                                                                               -1,
                                                                               false,
                                                                               false,
                                                                               1,
                                                                               1,
                                                                               pairs,
                                                                               false);
      liveConfig.getClusterConfigurations().add(ccc0);
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveServer = createLiveServer();

      installSecurity(liveServer);
   }

   protected void beforeRestart(TestableServer server)
   {
      installSecurity(server);
   }


   /**
    * @return
    */
   protected HornetQSecurityManager installSecurity(TestableServer server)
   {
      HornetQSecurityManager securityManager = server.getServer().getSecurityManager();
      securityManager.addUser("a", "b");
      Role role = new Role("arole", true, true, true, true, true, true, true);
      Set<Role> roles = new HashSet<Role>();
      roles.add(role);
      server.getServer().getSecurityRepository().addMatch("#", roles);
      securityManager.addRole("a", "arole");
      return securityManager;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
