/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.core.server.impl;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EmbeddedServerTest
{
   private static final String SERVER_LOCK_NAME = "server.lock";
   private static final String SERVER_JOURNAL_DIR = "target/data/journal";

   private HornetQServer server;
   private Configuration configuration;

   @Before
   public void setup()
   {
      configuration = new ConfigurationImpl();
      configuration.setJournalDirectory(SERVER_JOURNAL_DIR);
      configuration.setPersistenceEnabled(false);
      configuration.setSecurityEnabled(false);
      configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

      server = HornetQServers.newHornetQServer(configuration);
      try
      {
         server.start();
      }
      catch (Exception e)
      {
         Assert.fail();
      }
   }

   @After
   public void teardown()
   {
      try
      {
         server.stop();
      }
      catch (Exception e)
      {
         // Do Nothing
      }
   }

   @Test
   public void testNoLockFileWithPersistenceFalse()
   {
      Path journalDir = Paths.get(SERVER_JOURNAL_DIR, SERVER_LOCK_NAME);
      boolean lockExists = Files.exists(journalDir);
      Assert.assertFalse(lockExists);
   }
}
