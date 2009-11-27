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

package org.hornetq.tests.integration.cluster.failover;

import junit.framework.TestSuite;

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;

/**
 * A LargeMessageFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeMessageFailoverTest extends FailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static TestSuite suite()
   {
      TestSuite suite = new TestSuite();

      suite.addTest(new LargeMessageFailoverTest("testNonTransacted"));
      suite.addTest(new LargeMessageFailoverTest("testTransactedMessagesSentSoRollback"));
      suite.addTest(new LargeMessageFailoverTest("testTransactedMessagesNotSentSoNoRollback"));
      suite.addTest(new LargeMessageFailoverTest("testTransactedMessagesConsumedSoRollback"));
      suite.addTest(new LargeMessageFailoverTest("testTransactedMessagesNotConsumedSoNoRollback"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesSentSoRollbackOnEnd"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesSentSoRollbackOnPrepare"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesSentSoRollbackOnCommit"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesNotSentSoNoRollbackOnCommit"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesConsumedSoRollbackOnEnd"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesConsumedSoRollbackOnPrepare"));
      suite.addTest(new LargeMessageFailoverTest("testXAMessagesConsumedSoRollbackOnCommit"));
      suite.addTest(new LargeMessageFailoverTest("testCreateNewFactoryAfterFailover"));

      // Those tests are temporarily disabled for LargeMessage
      // suite.addTest(new LargeMessageFailoverTest("testFailoverMultipleSessionsWithConsumers"));
      // suite.addTest(new LargeMessageFailoverTest("testFailWithBrowser"));
      // suite.addTest(new LargeMessageFailoverTest("testFailThenReceiveMoreMessagesAfterFailover"));
      // suite.addTest(new LargeMessageFailoverTest("testFailThenReceiveMoreMessagesAfterFailover2"));

      suite.addTest(new LargeMessageFailoverTest("testForceBlockingReturn"));
      suite.addTest(new LargeMessageFailoverTest("testCommitOccurredUnblockedAndResendNoDuplicates"));
      suite.addTest(new LargeMessageFailoverTest("testCommitDidNotOccurUnblockedAndResend"));
      return suite;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @param name
    */
   public LargeMessageFailoverTest(String name)
   {
      super(name);
   }

   /**
    * 
    */
   public LargeMessageFailoverTest()
   {
      super();
   }

   /**
    * @param i
    * @param message
    */
   protected void assertMessageBody(int i, ClientMessage message)
   {
      HornetQBuffer buffer = message.getBodyBuffer();

      for (int j = 0; j < ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3; j++)
      {
         assertEquals(buffer.readByte(), getSamplebyte(j));
      }
   }

   /**
    * @param i
    * @param message
    */
   protected void setBody(int i, ClientMessage message) throws Exception
   {
      message.setBodyInputStream(createFakeLargeStream(ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
