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

package org.hornetq.tests.integration.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.server.Queue;
import org.hornetq.jms.persistence.JMSStorageManager;
import org.hornetq.jms.persistence.impl.journal.JMSJournalStorageManagerImpl;
import org.hornetq.tests.unit.core.server.impl.fakes.FakePostOffice;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.OrderedExecutorFactory;
import org.hornetq.utils.TimeAndCounterIDGenerator;

/**
 * A StorageManagerTestBase
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public abstract class StorageManagerTestBase extends ServiceTestBase
{

   protected ExecutorService executor;

   protected ExecutorFactory execFactory;

   protected JournalStorageManager journal;

   protected JMSStorageManager jmsJournal;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      executor = Executors.newCachedThreadPool();

      execFactory = new OrderedExecutorFactory(executor);

      File testdir = new File(getTestDir());

      deleteDirectory(testdir);
   }

   @Override
   protected void tearDown() throws Exception
   {
      Exception exception = null;

      if (journal != null)
      {
         try
         {
            journal.stop();
         }
         catch (Exception e)
         {
            exception = e;
         }

         journal = null;
      }

      if (jmsJournal != null)
      {
         try
         {
            jmsJournal.stop();
         }
         catch (Exception e)
         {
            if (exception != null)
               exception = e;
         }

         jmsJournal = null;
      }
      executor.shutdown();

      super.tearDown();
      if (exception != null)
         throw exception;
   }

   /**
    * @throws Exception
    */
   protected void createStorage() throws Exception
   {
      Configuration configuration = createDefaultConfig();

      journal = createJournalStorageManager(configuration);

      journal.start();

      journal.loadBindingJournal(new ArrayList<QueueBindingInfo>(), new ArrayList<GroupingInfo>());

      Map<Long, Queue> queues = new HashMap<Long, Queue>();

      journal.loadMessageJournal(new FakePostOffice(), null, null, queues, null, null, null, null);
   }

   /**
    * @param configuration
    */
   protected JournalStorageManager createJournalStorageManager(Configuration configuration)
   {
      JournalStorageManager jsm = new JournalStorageManager(configuration, execFactory, null);
      addHornetQComponent(jsm);
      return jsm;
   }

   /**
    * @throws Exception
    */
   protected void createJMSStorage() throws Exception
   {
      Configuration configuration = createDefaultConfig();

      jmsJournal = new JMSJournalStorageManagerImpl(new TimeAndCounterIDGenerator(), configuration, null);

      jmsJournal.start();

      jmsJournal.load();
   }
}
