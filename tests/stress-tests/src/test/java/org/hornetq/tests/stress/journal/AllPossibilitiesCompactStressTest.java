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

package org.hornetq.tests.stress.journal;


/**
 * A NIORandomCompactTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class AllPossibilitiesCompactStressTest extends MixupCompactorTestBase
{

   public void internalTest() throws Exception
   {
      createJournal();

      startJournal();

      loadAndCheck();

      long consumerTX = idGen.generateID();

      long firstID = idGen.generateID();

      long appendTX = idGen.generateID();

      long addedRecord = idGen.generateID();

      long addRecord2 = idGen.generateID();

      long addRecord3 = idGen.generateID();

      long addRecord4 = idGen.generateID();

      long addRecordStay = idGen.generateID();

      long addRecord5 = idGen.generateID();

      long rollbackTx = idGen.generateID();

      long rollbackAdd = idGen.generateID();

      add(addRecordStay);

      add(addRecord2);

      add(addRecord4);

      update(addRecord2);

      addTx(consumerTX, firstID);

      updateTx(consumerTX, addRecord4);

      addTx(consumerTX, addRecord5);

      addTx(appendTX, addedRecord);

      commit(appendTX);

      updateTx(consumerTX, addedRecord);

      commit(consumerTX);

      delete(addRecord4);

      delete(addedRecord);

      add(addRecord3);

      addTx(rollbackTx, rollbackAdd);

      long updateTX = idGen.generateID();

      updateTx(updateTX, addRecord3);

      commit(updateTX);

      updateTx(rollbackTx, rollbackAdd);

      delete(addRecord5);

      rollback(rollbackTx);

      checkJournalOperation();

      stopJournal();

      createJournal();

      startJournal();

      loadAndCheck();

      stopJournal();
   }

}
