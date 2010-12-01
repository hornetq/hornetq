/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.tests.integration.cluster;

import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.FileLockNodeManager;
import org.hornetq.tests.util.SpawnedVMSupport;
import org.hornetq.utils.UUID;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *         Date: Oct 18, 2010
 *         Time: 10:34:25 AM
 */
public class RealNodeManagerTest extends NodeManagerTest
{
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      File file = new File(".", "server.lock");
      if(file.exists())
      {
         file.delete();
      }
   }

   public void testId() throws Exception
   {
      NodeManager nodeManager = new FileLockNodeManager(".");
      nodeManager.start();
      UUID id1 = nodeManager.getUUID();
      nodeManager.stop();
      nodeManager.start();
      assertEqualsByteArrays(id1.asBytes(), nodeManager.getUUID().asBytes());
      nodeManager.stop();
   }
   @Override
   public void performWork(NodeManagerAction... actions) throws Exception
   {
      List<Process> processes = new ArrayList<Process>();
      for (NodeManagerAction action : actions)
      {
         Process p = SpawnedVMSupport.spawnVM(NodeManagerAction.class.getName(),"-Xms512m -Xmx512m ", new String[0], true, true,action.getWork());
         processes.add(p);
      }
      for (Process process : processes)
      {
         process.waitFor();
      }
      for (Process process : processes)
      {
         if(process.exitValue() == 9)
         {
            fail("failed see output");
         }
      }

   }
}
