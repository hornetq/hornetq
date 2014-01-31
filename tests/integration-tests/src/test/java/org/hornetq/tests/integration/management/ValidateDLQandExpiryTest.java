/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.tests.integration.management;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.logging.AssertionLoggerHandler;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Test;

/**
 * If you want to debug this on an IDE you need to set this attributes on your JDK arguments.
 * Replace checkout-folder by the place where you cloned hornetq and add this:
 * <p/>
 * -Dlogging.configuration=file:/checkout-folder/hornetq/tests/config/logging.properties -Djava.util.logging.manager=org.jboss.logmanager.LogManager
 *
 * @author Clebert Suconic
 */
public class ValidateDLQandExpiryTest extends ServiceTestBase
{

   public static final SimpleString IDONTEXIST = SimpleString.toSimpleString("IDONTEXIST");
   public static final SimpleString SOMEQUEUE = SimpleString.toSimpleString("SOMEQUEUE");
   public static final SimpleString DLQ_NON_EXISTENT = SimpleString.toSimpleString("DLQNonEXISTENT");

   /**
    * This test is validating for logging output, for that reason the test is starting the server
    * and doing a full cycle instead of using setUps and tearDowns.
    */
   @Test
   public void testExpiryAndDLQ() throws Exception
   {

      AssertionLoggerHandler.startCapture();

      try
      {
         Configuration config = createDefaultConfig(false);
         AddressSettings settings = new AddressSettings();
         settings.setExpiryAddress(IDONTEXIST);
         settings.setDeadLetterAddress(DLQ_NON_EXISTENT);
         config.getAddressesSettings().put("#", settings);
         HornetQServer server = HornetQServers.newHornetQServer(config);

         try
         {
            server.start();

            assertFalse(server.isAddressBound(IDONTEXIST.toString()));

            server.createQueue(SOMEQUEUE, SOMEQUEUE, null, true, false);

            assertTrue(AssertionLoggerHandler.findText("HQ222179", IDONTEXIST.toString()));

            AssertionLoggerHandler.clear();

            settings = new AddressSettings();
            settings.setExpiryAddress(SOMEQUEUE);
            settings.setDeadLetterAddress(SOMEQUEUE);

            server.getAddressSettingsRepository().addMatch("newQueue", settings);

            // There shouldn't be anything wrong now
            assertFalse(AssertionLoggerHandler.findText("HQ222179"));
            assertFalse(AssertionLoggerHandler.findText("HQ222180"));

            settings = new AddressSettings();
            settings.setExpiryAddress(SOMEQUEUE);
            settings.setDeadLetterAddress(DLQ_NON_EXISTENT);
            server.getAddressSettingsRepository().addMatch("nova", settings);

            // That has been logged already.. it shouldn't log again
            assertFalse(AssertionLoggerHandler.findText("HQ222180", DLQ_NON_EXISTENT.toString()));

            settings = new AddressSettings();
            settings.setExpiryAddress(SOMEQUEUE);
            settings.setDeadLetterAddress(DLQ_NON_EXISTENT.concat("2"));
            server.getAddressSettingsRepository().addMatch("nova2", settings);

            // It should log on this one.. it's new
            assertTrue(AssertionLoggerHandler.findText("HQ222180", DLQ_NON_EXISTENT.concat("2").toString()));

         }
         finally
         {
            try
            {
               server.stop();
            }
            catch (Exception e)
            {
            }
         }
      }
      finally
      {
         AssertionLoggerHandler.stopCapture();
      }
   }
}
