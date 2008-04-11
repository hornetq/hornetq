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

package org.jboss.test.messaging.tools.misc;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * A configurable SecurityManager, that, once installed, can selectively allow or disallow various
 * permissions.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ConfigurableSecurityManager extends SecurityManager
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private List disallowing;

   // Constructors ---------------------------------------------------------------------------------

   public ConfigurableSecurityManager()
   {
      disallowing = new ArrayList();
   }

   // SecurityManager overrides --------------------------------------------------------------------

   public void checkPermission(Permission perm)
   {
      for(Iterator i = disallowing.iterator(); i.hasNext(); )
      {
         PermissionActionHolder pat = (PermissionActionHolder)i.next();
         Class deniedPermissionClass = pat.getPermissionClass();
         String deniedAction = pat.getAction();

         if (!deniedPermissionClass.isAssignableFrom(perm.getClass()))
         {
            continue;
         }

         StringTokenizer st = new StringTokenizer(perm.getActions(), ", ");

         if (!st.hasMoreTokens())
         {
            throw new SecurityException(this + " does not allow " + perm);
         }

         for(; st.hasMoreTokens(); )
         {
            String action = st.nextToken();
            if (deniedAction.equals(action))
            {
               throw new SecurityException(
                  this + " does not allow " + perm + ", action " + action);
            }
         }
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public void disallow(Class permissionClass, String action)
   {
      if (!Permission.class.isAssignableFrom(permissionClass))
      {
         throw new IllegalArgumentException(permissionClass + " is not a Permission");
      }

      disallowing.add(new PermissionActionHolder(permissionClass, action));
   }

   public void clear()
   {
      disallowing.clear();
   }

   public String toString()
   {
      return "ConfigurableSecurityManager[" +
         Integer.toHexString(System.identityHashCode(this)) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   private class PermissionActionHolder
   {
      private Class permissionClass;
      private String action;

      public PermissionActionHolder(Class permissionClass, String action)
      {
         this.permissionClass = permissionClass;
         this.action = action;
      }

      public Class getPermissionClass()
      {
         return permissionClass;
      }

      public String getAction()
      {
         return action;
      }
   }

}
