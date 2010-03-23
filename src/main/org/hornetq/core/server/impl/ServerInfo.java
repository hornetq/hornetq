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

package org.hornetq.core.server.impl;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Date;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.utils.SizeFormatterUtil;

/**
 * A ServerInfo
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ServerInfo
{
   private final HornetQServer server;

   private final PagingManager pagingManager;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ServerInfo(final HornetQServer server, final PagingManager pagingManager)
   {
      this.server = server;
      this.pagingManager = pagingManager;
   }

   // Public --------------------------------------------------------

   public String dump()
   {
      long maxMemory = Runtime.getRuntime().maxMemory();
      long totalMemory = Runtime.getRuntime().totalMemory();
      long freeMemory = Runtime.getRuntime().freeMemory();
      long availableMemory = freeMemory + maxMemory - totalMemory;
      double availableMemoryPercent = 100.0 * availableMemory / maxMemory;
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

      String info = "\n**** Server Dump ****\n";
      info += String.format("date:            %s\n", new Date());
      info += String.format("free memory:      %s\n", SizeFormatterUtil.sizeof(freeMemory));
      info += String.format("max memory:       %s\n", SizeFormatterUtil.sizeof(maxMemory));
      info += String.format("total memory:     %s\n", SizeFormatterUtil.sizeof(totalMemory));
      info += String.format("available memory: %.2f%%\n", availableMemoryPercent);
      info += appendPagingInfos();
      info += String.format("# of thread:     %d\n", threadMXBean.getThreadCount());
      info += String.format("# of conns:      %d\n", server.getConnectionCount());
      info += "********************\n";
      return info;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private String appendPagingInfos()
   {
      String info = "";
      
      for (SimpleString storeName : pagingManager.getStoreNames())
      {
         PagingStore pageStore;
         try
         {
            pageStore = pagingManager.getPageStore(storeName);
            info += String.format("\t%s: %s\n",
                                  storeName,
                                  SizeFormatterUtil.sizeof(pageStore.getPageSizeBytes() * pageStore.getNumberOfPages()));
         }
         catch (Exception e)
         {
            info += String.format("\t%s: %s\n", storeName, e.getMessage());
         }
      }
      return info;
   }

   // Inner classes -------------------------------------------------

}
