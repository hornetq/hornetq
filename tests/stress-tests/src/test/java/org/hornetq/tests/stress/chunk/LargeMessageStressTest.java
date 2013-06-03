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

package org.hornetq.tests.stress.chunk;

import org.junit.Test;

import org.hornetq.tests.integration.largemessage.LargeMessageTestBase;

/**
 * A MessageChunkSoakTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Oct 27, 2008 5:07:05 PM
 *
 *
 */
public class LargeMessageStressTest extends LargeMessageTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testMessageChunkFilePersistenceOneHugeMessage() throws Exception
   {
      testChunks(false,
                 false,
                 false,
                 true,
                 true,
                 false,
                 false,
                 false,
                 true,
                 1,
                 200 * 1024l * 1024l + 1024l,
                 120000,
                 0,
                 10 * 1024 * 1024,
                 1024 * 1024);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
