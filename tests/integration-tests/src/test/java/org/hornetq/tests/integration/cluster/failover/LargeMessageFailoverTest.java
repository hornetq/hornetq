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

import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ServerLocatorInternal;

/**
 * A LargeMessageFailoverTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeMessageFailoverTest extends FailoverTest
{

   @Override
   public void testLiveAndBackupLiveComesBackNewFactory() throws Exception
   {
      // skip test because it triggers OutOfMemoryError.
      Thread.sleep(1000);
   }

   @Override
   public void testLiveAndBackupBackupComesBackNewFactory() throws Exception
   {
      // skip test because it triggers OutOfMemoryError.
      Thread.sleep(1000);
   }

   /**
    * @param i
    * @param message
    */
   @Override
   protected void assertMessageBody(final int i, final ClientMessage message)
   {
      assertLargeMessageBody(i, message);
   }


   @Override
   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = super.getServerLocator();
      locator.setMinLargeMessageSize(MIN_LARGE_MESSAGE);
      return (ServerLocatorInternal)locator;
   }

   /**
    * @param i
    * @param message
    */
   @Override
   protected void setBody(final int i, final ClientMessage message)
   {
      setLargeMessageBody(i, message);
   }
}
