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

import junit.framework.Assert;
import junit.framework.TestSuite;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.tests.util.UnitTestCase;

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
      suite.addTest(new LargeMessageFailoverTest("testFailoverMultipleSessionsWithConsumers"));
      suite.addTest(new LargeMessageFailoverTest("testFailWithBrowser"));
      suite.addTest(new LargeMessageFailoverTest("testFailThenReceiveMoreMessagesAfterFailover"));
      suite.addTest(new LargeMessageFailoverTest("testFailThenReceiveMoreMessagesAfterFailover2"));

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
   public LargeMessageFailoverTest(final String name)
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
   @Override
   protected void assertMessageBody(final int i, final ClientMessage message)
   {
      HornetQBuffer buffer = message.getBodyBuffer();

      for (int j = 0; j < HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3; j++)
      {
         Assert.assertEquals(buffer.readByte(), UnitTestCase.getSamplebyte(j));
      }
   }

   /**
    * @param i
    * @param message
    */
   @Override
   protected void setBody(final int i, final ClientMessage message) throws Exception
   {
      message.setBodyInputStream(UnitTestCase.createFakeLargeStream(HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE * 3));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
