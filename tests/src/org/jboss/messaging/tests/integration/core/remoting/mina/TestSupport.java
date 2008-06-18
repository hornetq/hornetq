/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.tests.integration.core.remoting.mina;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * @version <tt>$Revision$</tt>
 */
public abstract class TestSupport
{
   // Constants -----------------------------------------------------

   public static final int MANY_MESSAGES = 50000;

   public static final int PING_INTERVAL = 2000; // in seconds

   public static final int PING_TIMEOUT = 1000; // in seconds

   public static final long REQRES_TIMEOUT = 2; // in seconds

   public static final int PORT = 9090;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String reverse(String text)
   {
      // Reverse text
      StringBuffer buf = new StringBuffer(text.length());
      for (int i = text.length() - 1; i >= 0; i--)
      {
         buf.append(text.charAt(i));
      }
      return buf.toString();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
