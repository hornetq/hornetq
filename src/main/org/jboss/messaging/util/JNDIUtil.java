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
package org.jboss.messaging.util;

import java.util.StringTokenizer;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JNDIUtil
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   /**
    * Create a context path recursively.
    */
   public static Context createContext(Context c, String path) throws NamingException
   {
      Context crtContext = c;
      for(StringTokenizer st = new StringTokenizer(path, "/"); st.hasMoreTokens(); )
      {
         String tok = st.nextToken();

         try
         {
            Object o = crtContext.lookup(tok);
            if (!(o instanceof Context))
            {
               throw new NamingException("Path " + path + " overwrites and already bound object");
            }
            crtContext = (Context)o;
            continue;
         }
         catch(NameNotFoundException e)
         {
            // OK
         }
         crtContext = crtContext.createSubcontext(tok);
      }
      return crtContext;
   }

   public static void tearDownRecursively(Context c) throws Exception
   {
      for(NamingEnumeration ne = c.listBindings(""); ne.hasMore(); )
      {
         Binding b = (Binding)ne.next();
         String name = b.getName();
         Object object = b.getObject();
         if (object instanceof Context)
         {
            tearDownRecursively((Context)object);
         }
         c.unbind(name);
      }
   }

   /**
    * Context.rebind() requires that all intermediate contexts and the target context (that named by
    * all but terminal atomic component of the name) must already exist, otherwise
    * NameNotFoundException is thrown. This method behaves similar to Context.rebind(), but creates
    * intermediate contexts, if necessary.
    */
   public static void rebind(Context c, String jndiName, Object o) throws NamingException
   {
      Context context = c;
      String name = jndiName;

      int idx = jndiName.lastIndexOf('/');
      if (idx != -1)
      {
         context = createContext(c, jndiName.substring(0, idx));
         name = jndiName.substring(idx + 1);
      }
      boolean failed=false;
      try
      {
         context.rebind(name,o);
      }
      catch (Exception ignored)
      {
         failed=true;
      }
      if (failed)
      {
         context.bind(name, o);
      }
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
