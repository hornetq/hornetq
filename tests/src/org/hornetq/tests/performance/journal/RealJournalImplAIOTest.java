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

package org.hornetq.tests.performance.journal;

import java.io.File;

import junit.framework.TestSuite;

import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A RealJournalImplTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class RealJournalImplAIOTest extends JournalImplTestUnit
{
   private static final Logger log = Logger.getLogger(RealJournalImplAIOTest.class);

   public static TestSuite suite()
   {
      return UnitTestCase.createAIOTestSuite(RealJournalImplAIOTest.class);
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   protected SequentialFileFactory getFileFactory() throws Exception
   {
      File file = new File(getTestDir());

      RealJournalImplAIOTest.log.debug("deleting directory " + file);

      deleteDirectory(file);

      file.mkdir();

      return new AIOSequentialFileFactory(getTestDir());
   }

}
