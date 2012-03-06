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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.logging.Logger;
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

   private static final Logger log = Logger.getLogger(LargeMessageFailoverTest.class);

   
   private static final int MIN_LARGE_MESSAGE = 1024;
   
   private static final int LARGE_MESSAGE_SIZE = MIN_LARGE_MESSAGE * 3;
   
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

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

   @Override
   public void testLiveAndBackupLiveComesBackNewFactory() throws Exception
   {
      Thread.sleep(1000);
   }

   @Override
   public void testLiveAndBackupBackupComesBackNewFactory() throws Exception
   {
      Thread.sleep(1000);
   }

   /**
    * @param i
    * @param message
    */
   @Override
   protected void assertMessageBody(final int i, final ClientMessage message)
   {
      HornetQBuffer buffer = message.getBodyBuffer();

      for (int j = 0; j < LARGE_MESSAGE_SIZE; j++)
      {
         Assert.assertEquals("equal at " + j, buffer.readByte(), UnitTestCase.getSamplebyte(j));
      }
   }
   

   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = super.getServerLocator();
      locator.setMinLargeMessageSize(LARGE_MESSAGE_SIZE);
      return (ServerLocatorInternal) locator;
   }



   /**
    * @param i
    * @param message
    */
   @Override
   protected void setBody(final int i, final ClientMessage message) throws Exception
   {
      message.setBodyInputStream(UnitTestCase.createFakeLargeStream(LARGE_MESSAGE_SIZE));
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
