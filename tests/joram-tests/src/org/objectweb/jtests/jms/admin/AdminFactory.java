/*
 * JORAM: Java(TM) Open Reliable Asynchronous Messaging
 * Copyright (C) 2002 INRIA
 * Contact: joram-team@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 * 
 * Initial developer(s): Jeff Mesnil (jmesnil@gmail.com)
 * Contributor(s): ______________________________________.
 */

package org.objectweb.jtests.jms.admin;

import java.util.Properties;

import org.jboss.util.NestedRuntimeException;

public class AdminFactory
{
   private static final String PROP_NAME = "jms.provider.admin.class";

   protected static String getAdminClassName(Properties props)
   {
      String adminClassName = props.getProperty(PROP_NAME);
      return adminClassName;
   }

   public static Admin getAdmin(Properties props)
   {
      String adminClassName = getAdminClassName(props);
      Admin admin = null;
      if (adminClassName == null)
      {
         throw new NestedRuntimeException("Property " + PROP_NAME + " has not been found in input props");
      }
      try
      {
         Class adminClass = Class.forName(adminClassName);
         admin = (Admin) adminClass.newInstance();
      }
      catch (ClassNotFoundException e)
      {
         throw new NestedRuntimeException("Class " + adminClassName + " not found.", e);
      }
      catch (Exception e)
      {
         throw new NestedRuntimeException(e);
      }
      return admin;
   }
}
