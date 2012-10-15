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

package org.hornetq.tests.timing.core.journal.impl;

import java.io.File;

import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;

/**
 *
 * A RealJournalImplTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class NIOJournalImplTest extends JournalImplTestUnit
{
   private static final Logger log = Logger.getLogger(NIOJournalImplTest.class);

   protected String journalDir = System.getProperty("java.io.tmpdir", "/tmp") + "/journal-test";

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(journalDir);

      NIOJournalImplTest.log.debug("deleting directory " + journalDir);

      deleteDirectory(file);

      file.mkdir();

      return new NIOSequentialFileFactory(journalDir);
   }

}
