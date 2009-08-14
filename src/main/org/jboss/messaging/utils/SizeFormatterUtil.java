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

package org.jboss.messaging.utils;

/**
 * A SizeFormatterUtil
 *
 * @author <a href="mailto:jmesnil@gmail.com>Jeff Mesnil</a>
 *
 *
 */
public class SizeFormatterUtil
{

   // Constants -----------------------------------------------------

   private static long oneKiB = 1024;

   private static long oneMiB = oneKiB * 1024;

   private static long oneGiB = oneMiB * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String sizeof(long size)
   {
      double s = Long.valueOf(size).doubleValue();
      String suffix = "B";
      if (s > oneGiB)
      {
         s /= oneGiB;
         suffix = "GiB";
      }
      else if (s > oneMiB)
      {
         s /= oneMiB;
         suffix = "MiB";
      }
      else if (s > oneKiB)
      {
         s /= oneKiB;
         suffix = "kiB";
      }
      return String.format("%.2f %s", s, suffix);
   }
   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
