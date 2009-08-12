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

package org.jboss.messaging.tests.integration.persistence;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.QueueBindingInfo;
import org.jboss.messaging.core.persistence.impl.journal.JournalStorageManager;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;

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
   private static final Logger log = Logger.getLogger(RestartSMTest.class);
                                                      
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testRestartStorageManager() throws Exception
   {
      File testdir = new File(getTestDir());
      deleteDirectory(testdir);

      Configuration configuration = createConfigForJournal();

      configuration.start();

      configuration.setJournalType(JournalType.ASYNCIO);

      final JournalStorageManager journal = new JournalStorageManager(configuration, Executors.newCachedThreadPool());
      try
      {

         journal.start();

         List<QueueBindingInfo> queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos);

         Map<Long, Queue> queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(null, null, queues, null);

         journal.stop();

         deleteDirectory(testdir);

         journal.start();

         queues = new HashMap<Long, Queue>();

         journal.loadMessageJournal(null, null, queues, null);

         queueBindingInfos = new ArrayList<QueueBindingInfo>();

         journal.loadBindingJournal(queueBindingInfos);

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
            log.warn(ex.getMessage(), ex);
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
