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
package org.jboss.test.messaging.tools.container;

import java.rmi.Naming;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A utility to stop runaway rmi servers.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2876 $</tt>
 *
 * $Id: StopRMIServer.java 2876 2007-07-11 18:00:52Z timfox $
 */
public class StopRMIServer
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(StopRMIServer.class);
   
   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {

      String host = System.getProperty("test.bind.address");
      if (host == null)
      {
         host = "localhost";
      }
      
      int index = -1; // -1 implies kill all servers

      String serverIndex = System.getProperty("test.server.index");
      
      if (serverIndex != null)
      {     
      	index = Integer.parseInt(serverIndex);
      }
            
      if (index != -1)
      {
      	killServer(host, index);
      }
      else
      {
      	//Kill em all
      	for (int i = 0; i < ServerManagement.MAX_SERVER_COUNT; i++)
      	{
      		killServer(host, i);
      	}
      }           
   }
      
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   private static void killServer(String host, int index) throws Exception
   {
   	String name =
         "//" + host + ":" + RMITestServer.DEFAULT_REGISTRY_PORT + "/" +
         RMITestServer.RMI_SERVER_PREFIX + index;

      Server server;
      try
      {
         server = (Server)Naming.lookup(name);
      }
      catch(Throwable t)
      {
      	//Ignore
         return;
      }
      
      try
      {
      	server.kill();
      }
      catch (Throwable t)
      {      	
      	//Ignore
      }
      
      try
      {
         while(true)
         {
            server.ping();           
            Thread.sleep(100);
         }
      }
      catch (Throwable e)
      {
         //Ok
      }
      
      log.info("Killed remote server " + index);
   }


   // Inner classes -------------------------------------------------

}
