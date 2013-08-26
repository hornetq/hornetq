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

package org.hornetq.tests.integration.persistence;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.OrderedExecutorFactory;

/**
 * A DeleteMessagesRestartTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Mar 2, 2009 10:14:38 AM
 *
 *
 */
public class RestartSMTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Attributes ----------------------------------------------------

   ExecutorService executor;

   ExecutorFactory execFactory;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      executor = Executors.newCachedThreadPool();

      execFactory = new OrderedExecutorFactory(executor);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      executor.shutdown();

      super.tearDown();
   }

   @Test
   public void testRestartStorageManager() throws Exception
   {
      File testdir = new File(getTestDir());
      deleteDirectory(testdir);

      Configuration configuration = createDefaultConfig();

      PostOffice postOffice = new FakePostOffice();

      final JournalStorageManager journal = new JournalStorageManager(configuration, execFactory, null);

      try
      {

         journal.start();

         List<QueueBindingInfo> queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos, new ArrayList<GroupingInfo>());

         Map<Long, Queue> queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(postOffice, null, null, queues, null, null, null);

         journal.stop();

         deleteDirectory(testdir);

         journal.start();

         queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(postOffice, null, null, queues, null, null, null);

         queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos, new ArrayList<GroupingInfo>());

         journal.start();
      }
      finally
      {

         try
         {
            journal.stop();
         }
         catch (Exception ex)
         {
            RestartSMTest.log.warn(ex.getMessage(), ex);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
