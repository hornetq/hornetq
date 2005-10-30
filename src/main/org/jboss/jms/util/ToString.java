/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.jms.util;

import javax.jms.DeliveryMode;
import javax.jms.Session;

/**
 * A collection of static string convertors.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ToString
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static String deliveryMode(int m)
   {
      if (m == DeliveryMode.NON_PERSISTENT)
      {
         return "NON_PERSISTENT";
      }
      if (m == DeliveryMode.PERSISTENT)
      {
         return "PERSISTENT";
      }
      return "UNKNOWN";
   }

   public static String acknowledgmentMode(int ack)
   {
      if (ack == Session.AUTO_ACKNOWLEDGE)
      {
         return "AUTO_ACKNOWLEDGE";
      }
      if (ack == Session.CLIENT_ACKNOWLEDGE)
      {
         return "CLIENT_ACKNOWLEDGE";
      }
      if (ack == Session.DUPS_OK_ACKNOWLEDGE)
      {
         return "DUPS_OK_ACKNOWLEDGE";
      }
      return "UNKNOWN";
   }


   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
