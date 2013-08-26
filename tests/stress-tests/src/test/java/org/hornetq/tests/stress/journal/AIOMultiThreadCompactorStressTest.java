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

package org.hornetq.tests.stress.journal;
import org.junit.Before;

import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.server.JournalType;
import org.junit.BeforeClass;

/**
 * A AIOMultiThreadCompactorStressTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class AIOMultiThreadCompactorStressTest extends NIOMultiThreadCompactorStressTest
{

   @BeforeClass
   public static void hasAIO()
   {
      org.junit.Assume.assumeTrue("Test case needs AIO to run", AIOSequentialFileFactory.isSupported());
   }

   /**
    * @return
    */
   @Override
   protected JournalType getJournalType()
   {
      return JournalType.ASYNCIO;
   }

}
