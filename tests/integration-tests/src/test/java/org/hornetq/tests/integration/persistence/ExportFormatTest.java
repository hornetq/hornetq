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

import org.junit.Test;

import java.io.StringReader;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.journal.impl.ExportJournal;
import org.hornetq.core.journal.impl.ImportJournal;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ExportFormatTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ExportFormatTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Case the format was changed, and the change was agreed, use _testCreateFormat to recreate this field
   String bindingsFile = "#File,JournalFileImpl: (hornetq-bindings-1.bindings id = 1, recordID = 1)\n" +
         "operation@AddRecord,id@2,userRecordType@24,length@8,isUpdate@false,compactCount@0,data@AAAAAH____8=\n" +
         "operation@AddRecord,id@2,userRecordType@21,length@17,isUpdate@false,compactCount@0,data@AAAABEEAMQAAAAAEQQAxAAA=\n" +
         "operation@AddRecord,id@20,userRecordType@24,length@8,isUpdate@false,compactCount@0,data@AAAAAAAAABQ=\n" +
         "#File,JournalFileImpl: (hornetq-bindings-2.bindings id = 2, recordID = 2)";

   // Case the format was changed, and the change was agreed, use _testCreateFormat to recreate this field
   String journalFile = "#File,JournalFileImpl: (hornetq-data-1.hq id = 1, recordID = 1)\n" +
         "operation@AddRecordTX,txID@0,id@4,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABAEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP40EAQAAAAEAAAAGawBlAHkABgAAAAA=\n" +
         "operation@UpdateTX,txID@0,id@4,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecordTX,txID@0,id@5,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABQEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP5EEAQAAAAEAAAAGawBlAHkABgAAAAE=\n" +
         "operation@UpdateTX,txID@0,id@5,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecordTX,txID@0,id@6,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABgEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP5EEAQAAAAEAAAAGawBlAHkABgAAAAI=\n" +
         "operation@UpdateTX,txID@0,id@6,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecordTX,txID@0,id@7,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABwEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP5EEAQAAAAEAAAAGawBlAHkABgAAAAM=\n" +
         "operation@UpdateTX,txID@0,id@7,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecordTX,txID@0,id@8,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAACAEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP5EEAQAAAAEAAAAGawBlAHkABgAAAAQ=\n" +
         "operation@UpdateTX,txID@0,id@8,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@Commit,txID@0,numberOfRecords@10\n" +
         "operation@AddRecord,id@12,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADAEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP6gEAQAAAAEAAAAGawBlAHkABgAAAAU=\n" +
         "operation@Update,id@12,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecord,id@13,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADQEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP6oEAQAAAAEAAAAGawBlAHkABgAAAAY=\n" +
         "operation@Update,id@13,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecord,id@14,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADgEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP6sEAQAAAAEAAAAGawBlAHkABgAAAAc=\n" +
         "operation@Update,id@14,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecord,id@15,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADwEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP60EAQAAAAEAAAAGawBlAHkABgAAAAg=\n" +
         "operation@Update,id@15,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "operation@AddRecord,id@16,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAAEAEAAAAEQQAxAAAA_wAAAAAAAAAAAAABLLxYP64EAQAAAAEAAAAGawBlAHkABgAAAAk=\n" +
         "operation@Update,id@16,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAI=\n" +
         "#File,JournalFileImpl: (hornetq-data-2.hq id = 2, recordID = 2)";

   public void _testCreateFormat() throws Exception
   {
      HornetQServer server = createServer(true);
      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, false, false);
      session.createQueue("A1", "A1");

      ClientProducer producer = session.createProducer("A1");
      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         producer.send(msg);
      }
      session.commit();

      session.close();

      session = factory.createSession(false, true, true);

      producer = session.createProducer("A1");

      for (int i = 5; i < 10; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         producer.send(msg);
      }

      locator.close();
      server.stop();

      System.out.println("copy & paste the following as bindingsFile:");

      ExportJournal.exportJournal(getBindingsDir(), "hornetq-bindings", "bindings", 2, 1048576, System.out);

      System.out.println("copy & paste the following as dataFile:");

      ExportJournal.exportJournal(getJournalDir(), "hornetq-data", "hq", 2, 102400, System.out);
   }

   @Test
   public void testConsumeFromFormat() throws Exception
   {
      ImportJournal.importJournal(getJournalDir(), "hornetq-data", "hq", 2, 102400, new StringReader(journalFile));
      ImportJournal.importJournal(getBindingsDir(),
                                  "hornetq-bindings",
                                  "bindings",
                                  2,
                                  1048576,
                                  new StringReader(bindingsFile));

      HornetQServer server = createServer(true);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession();
      session.start();

      ClientConsumer consumer = session.createConsumer("A1");
      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
         assertEquals(i, msg.getIntProperty("key").intValue());
      }

      session.commit();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
