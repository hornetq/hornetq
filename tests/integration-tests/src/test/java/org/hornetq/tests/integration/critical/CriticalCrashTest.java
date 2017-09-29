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

package org.hornetq.tests.integration.critical;

import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Clebert Suconic
 */

public class CriticalCrashTest extends ServiceTestBase
{
   @Test
   public void testCrash() throws Exception
   {
      Process process = SpawnedVMSupport.spawnVM(CriticalCrashSpawned.class.getName(),
                               new String[] {"-Dbrokerconfig.criticalAnalyzer=true", "-Dbrokerconfig.criticalAnalyzerCheckPeriod=500", "-Dbrokerconfig.criticalAnalyzerTimeout=1000", "-Dbrokerconfig.criticalAnalyzerPolicy=HALT"},
                               new String[]{});

      Assert.assertEquals(70, process.waitFor());
   }
}
