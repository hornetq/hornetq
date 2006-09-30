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
package org.jboss.example.jms.common;

import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Util
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------


   public static boolean doesDestinationExist(String jndiName) throws Exception
   {
       return doesDestinationExist(jndiName, null);
   }

   public static boolean doesDestinationExist(String jndiName, InitialContext ic) throws Exception
   {
      if (ic==null)
          ic = new InitialContext();
      try
      {
         ic.lookup(jndiName);
      }
      catch(NameNotFoundException e)
      {
         return false;
      }
      return true;
   }

   public static void deployQueue(String jndiName) throws Exception
   {
       deployQueue(jndiName,null);
   }

   public static void deployQueue(String jndiName, InitialContext ic) throws Exception
   {
      MBeanServerConnection mBeanServer = lookupMBeanServerProxy(ic);

      ObjectName serverObjectName = new ObjectName("jboss.messaging:service=ServerPeer");

      String queueName = jndiName.substring(jndiName.lastIndexOf('/') + 1);

      mBeanServer.invoke(serverObjectName, "createQueue",
                         new Object[] {queueName, jndiName},
                         new String[] {"java.lang.String", "java.lang.String"});

      System.out.println("Queue " + jndiName + " deployed");
   }

   public static void undeployQueue(String jndiName) throws Exception
   {
       undeployQueue(jndiName,null);
   }

   public static void undeployQueue(String jndiName, InitialContext ic) throws Exception
   {
      MBeanServerConnection mBeanServer = lookupMBeanServerProxy(ic);

      ObjectName serverObjectName = new ObjectName("jboss.messaging:service=ServerPeer");

      String queueName = jndiName.substring(jndiName.lastIndexOf('/') + 1);

      mBeanServer.invoke(serverObjectName, "destroyQueue",
                         new Object[] {queueName},
                         new String[] {"java.lang.String"});

      System.out.println("Queue " + jndiName + " undeployed");
   }

   public static MBeanServerConnection lookupMBeanServerProxy(InitialContext ic) throws Exception
   {
      if (ic==null)
      {
        ic = new InitialContext();
      }
      MBeanServerConnection p = (MBeanServerConnection)ic.lookup("jmx/invoker/RMIAdaptor");
      //ic.close();
      return p;
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
