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

package org.hornetq.tests.integration.largemessage;

import java.util.concurrent.Executors;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ServerLargeMessageTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ServerLargeMessageTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testLargeMessageCopy() throws Exception
   {
      clearData();

      Configuration configuration = createDefaultConfig();

      configuration.start();

      configuration.setJournalType(JournalType.ASYNCIO);

      final JournalStorageManager journal = new JournalStorageManager(configuration, Executors.newCachedThreadPool());
      journal.start();

      LargeServerMessage msg = journal.createLargeMessage();
      msg.setMessageID(1);

      byte[] data = new byte[1024];

      for (int i = 0; i < 110; i++)
      {
         msg.addBytes(data);
      }

      ServerMessage msg2 = msg.copy(2);

      assertEquals(110 * 1024, msg.getBodySize());
      assertEquals(110 * 1024, msg2.getBodySize());

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
