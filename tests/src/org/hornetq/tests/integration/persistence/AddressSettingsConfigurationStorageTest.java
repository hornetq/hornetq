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

package org.hornetq.tests.integration.persistence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * A ConfigurationStorageTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class AddressSettingsConfigurationStorageTest extends StorageManagerTestBase
{

   private Map<SimpleString, PersistedAddressSetting> mapExpectedAddresses;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      mapExpectedAddresses = new HashMap<SimpleString, PersistedAddressSetting>();
   }

   @Override
   protected void tearDown() throws Exception
   {
      mapExpectedAddresses = null;

      super.tearDown();
   }

   protected void addAddress(JournalStorageManager journal, String address, AddressSettings setting) throws Exception
   {
      SimpleString str = new SimpleString(address);
      PersistedAddressSetting persistedSetting = new PersistedAddressSetting(str, setting);
      mapExpectedAddresses.put(str, persistedSetting);
      journal.storeAddressSetting(persistedSetting);
   }

   public void testStoreSecuritySettings() throws Exception
   {
      createStorage();

      AddressSettings setting = new AddressSettings();

      setting = new AddressSettings();

      setting.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      setting.setDeadLetterAddress(new SimpleString("some-test"));

      addAddress(journal, "a2", setting);

      journal.stop();

      createStorage();

      checkAddresses(journal);

      setting = new AddressSettings();

      setting.setDeadLetterAddress(new SimpleString("new-adddress"));

      // Replacing the first setting
      addAddress(journal, "a1", setting);

      journal.stop();

      createStorage();

      checkAddresses(journal);
      
      journal.stop();

      journal = null;

   }

   /**
    * @param journal
    * @throws Exception
    */
   private void checkAddresses(JournalStorageManager journal) throws Exception
   {
      List<PersistedAddressSetting> listSetting = journal.recoverAddressSettings();

      assertEquals(mapExpectedAddresses.size(), listSetting.size());

      for (PersistedAddressSetting el : listSetting)
      {
         PersistedAddressSetting el2 = mapExpectedAddresses.get(el.getAddressMatch());

         assertEquals(el.getAddressMatch(), el2.getAddressMatch());
         assertEquals(el.getSetting(), el2.getSetting());
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
