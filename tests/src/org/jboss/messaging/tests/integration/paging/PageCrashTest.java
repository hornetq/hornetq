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

package org.jboss.messaging.tests.integration.paging;

import java.io.File;
import java.util.HashMap;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.tests.integration.paging.remote.RemotePageCrashExecution;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.tests.util.SpawnedVMSupport;

/**
 * This test will make sure that a failing depage won't cause duplicated messages
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Jan 7, 2009 6:19:43 PM
 *
 *
 */
public class PageCrashTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCrashDuringDeleteFile() throws Exception
   {
      clearData();

      Process process = SpawnedVMSupport.spawnVM(RemotePageCrashExecution.class.getCanonicalName());
      process.waitFor();
      assertEquals("The remote process failed, test is invalid", RemotePageCrashExecution.OK, process.exitValue());

      File pageDir = new File(getPageDir());

      File directories[] = pageDir.listFiles();

      assertEquals(1, directories.length);

      // When depage happened, a new empty page was supposed to be opened, what will create 3 files
      assertEquals("Missing a file, supposed to have address.txt, 1st page and 2nd page",
                   3,
                   directories[0].list().length);

      Configuration config = createDefaultConfig();

      config.setPagingMaxGlobalSizeBytes(100 * 1024);
      config.setPagingDefaultSize(10 * 1024);

      MessagingService messagingService = createService(true, config, new HashMap<String, QueueSettings>());

      messagingService.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonPersistentSend(true);
         sf.setBlockOnPersistentSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         ClientConsumer consumer = session.createConsumer(RemotePageCrashExecution.ADDRESS);

         assertNull(consumer.receive(200));

         session.close();
      }
      finally
      {
         messagingService.stop();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
