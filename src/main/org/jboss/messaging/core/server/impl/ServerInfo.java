/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.util.Date;

import org.jboss.messaging.core.server.MessagingServer;

/**
 * A ServerInfo
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ServerInfo
{
   private final MessagingServer server;

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ServerInfo(final MessagingServer server)
   {
      this.server = server;
   }

   // Public --------------------------------------------------------

   public String dump()
   {
      MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
      MemoryUsage heapMemory = memory.getHeapMemoryUsage();
      MemoryUsage nonHeapMemory = memory.getHeapMemoryUsage();
      ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
      String info = "\n**** Server Dump ****\n";
      info += String.format("date:            %s\n", new Date());
      info += String.format("heap memory:     used=%s, max=%s\n",
                            sizeof(heapMemory.getUsed()),
                            sizeof(heapMemory.getMax()));
      info += String.format("non-heap memory: used=%s, max=%s\n",
                            sizeof(nonHeapMemory.getUsed()),
                            sizeof(nonHeapMemory.getMax()));
      info += String.format("paging memory:   %s\n", sizeof(server.getPagingTotalMemory()));
      info += String.format("# of thread:     %d\n", threadMXBean.getThreadCount());
      info += String.format("# of conns:      %d\n", server.getConnectionCount());
      info += "********************\n";
      return info;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static long oneKB = 1024;

   private static long oneMB = oneKB * 1024;

   private static long oneGB = oneMB * 1024;

   private static String sizeof(long size)
   {
      double s = Long.valueOf(size).doubleValue();
      String suffix = "B";
      if (s > oneGB)
      {
         s /= oneGB;
         suffix = "GB";
      }
      else if (s > oneMB)
      {
         s /= oneMB;
         suffix = "MB";
      }
      else if (s > oneKB)
      {
         s /= oneKB;
         suffix = "kB";
      }
      return String.format("%.2f %s", s, suffix);
   }

   // Inner classes -------------------------------------------------

}
