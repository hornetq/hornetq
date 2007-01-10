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
package org.jboss.test.messaging.tools.jmx;

import java.lang.reflect.Array;
import java.net.URLClassLoader;
import java.net.URL;

/**
 * We extend URLClassLoader just to prevent UnifiedLoaderRepository3 to generate spurious warning
 * (in this case): "Tried to add non-URLClassLoader. Ignored". Extending ClassLoader would be fine
 * otherwise.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClassLoaderJMXWrapper extends URLClassLoader implements ClassLoaderJMXWrapperMBean
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ClassLoader delegate;

   // Constructors --------------------------------------------------

   public ClassLoaderJMXWrapper(ClassLoader delegate)
   {
      super(new URL[0]);
      this.delegate = delegate;
   }

   // ClassLoaderJMXWrapperMBean implementation ---------------------

   public Class loadClass(String name) throws ClassNotFoundException
   {
      if (name.endsWith("[]"))
      {
         name = name.substring(0, name.length() - 2);
         
         //The classloader of an array type is the classloader of it's element (if non primitive)
         
         Class cl = delegate.loadClass(name);
         
         Object arr = Array.newInstance(cl, 0);
         
         return arr.getClass();
      }
      else
      {      
         return delegate.loadClass(name);
      }
   }

   // ClassLoader overrides -----------------------------------------

   public URL getResource(String name)
   {
      return delegate.getResource(name);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
