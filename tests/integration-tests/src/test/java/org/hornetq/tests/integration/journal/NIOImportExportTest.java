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

package org.hornetq.tests.integration.journal;
import org.junit.After;

import org.junit.Test;

import java.io.File;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.tests.unit.core.journal.impl.JournalImplTestBase;
import org.hornetq.tests.unit.core.journal.impl.fakes.SimpleEncoding;

/**
 * A NIOImportExportTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class NIOImportExportTest extends JournalImplTestBase
{

   /* (non-Javadoc)
    * @see org.hornetq.tests.unit.core.journal.impl.JournalImplTestBase#getFileFactory()
    */
   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(getTestDir());

      deleteDirectory(file);

      file.mkdir();

      return new NIOSequentialFileFactory(getTestDir(), true);
   }

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   @Test
   public void testExportImport() throws Exception
   {
      setup(10, 10 * 1024, true);

      createJournal();

      startJournal();

      load();

      add(1, 2);

      journal.forceMoveNextFile();

      delete(1, 2);

      add(3, 4);

      journal.forceMoveNextFile();

      addTx(5, 6, 7, 8);

      journal.forceMoveNextFile();

      addTx(5, 9);

      commit(5);

      journal.forceMoveNextFile();

      deleteTx(10, 6, 7, 8, 9);

      commit(10);

      addTx(11, 11, 12);
      updateTx(11, 11, 12);
      commit(11);

      journal.forceMoveNextFile();

      update(11, 12);

      stopJournal();

      exportImportJournal();

      createJournal();

      startJournal();

      loadAndCheck();

   }

   @Test
   public void testExportImport3() throws Exception
   {
      setup(10, 10 * 1024, true);

      createJournal();

      startJournal();

      load();

      add(1, 2);

      journal.forceMoveNextFile();

      delete(1, 2);

      add(3, 4);

      journal.forceMoveNextFile();

      addTx(5, 6, 7, 8);

      journal.forceMoveNextFile();

      addTx(5, 9);

      commit(5);

      journal.forceMoveNextFile();

      deleteTx(10, 6, 7, 8, 9);

      commit(10);

      addTx(11, 12, 13);

      EncodingSupport xid = new SimpleEncoding(10, (byte)0);
      prepare(11, xid);

      stopJournal();

      exportImportJournal();

      createJournal();

      startJournal();

      loadAndCheck();

      commit(11);

      stopJournal();

      exportImportJournal();

      createJournal();

      startJournal();

      loadAndCheck();

   }

   @Test
   public void testExportImport2() throws Exception
   {
      setup(10, 10 * 1024, true);

      createJournal();

      startJournal();

      load();

      add(1);

      stopJournal();

      exportImportJournal();

      createJournal();

      startJournal();

      loadAndCheck();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
