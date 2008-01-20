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

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.SecureRandom;

import org.jboss.messaging.util.Logger;

/**
 * PortUtil is a set of utilities for dealing with TCP/IP ports
 *
 * @author <a href="mailto:jhaynie@vocalocity.net">Jeff Haynie</a>
 * @author <a href="mailto:tom@jboss.org">Tom Elrod</a>
 * @version $Revision$
 */
public class PortUtil
{
   private static final Logger log = Logger.getLogger(PortUtil.class);
   private static final int MIN_UNPRIVILEGED_PORT = 1024;
   private static final int MAX_LEGAL_PORT = 65535;

   private static int portCounter = 0;
   private static int retryMax = 50;

   static
   {
      portCounter = getRandomStartingPort();
   }

   /**
    * Checks to see if the specified port is free.
    *
    * @param p
    * @return true if available, false if already in use
    */
   public static boolean checkPort(int p, String host)
   {
      boolean available = true;
      ServerSocket socket = null;
      try
      {
         InetAddress inetAddress = InetAddress.getByName(host);
         socket = new ServerSocket(p, 0, inetAddress);
      }
      catch(IOException e)
      {
         log.debug("port " + p + " already in use.  Will try another.");
         available = false;
      }
      finally
      {
         if(socket != null)
         {
            try
            {
               socket.close();
            }
            catch(IOException e)
            {

            }
         }
      }
      return available;

   }

   /**
    * Will try to find a port that is not in use up to 50 tries, at which point,
    * will throw an exception.
    * @return
    */
   public static int findFreePort(String host) throws IOException
   {
      Integer port = null;
      int tryCount = 0;
      while(port == null && tryCount < retryMax)
      {
         port = getFreePort(host);
         if(port != null)
         {
            // validate port again, just in case two instances start on the port at same time.
            if(!checkPort(port.intValue(), host))
            {
               port = null;
            }
         }
         tryCount++;
      }
      if(tryCount >= retryMax)
      {
         throw new IOException("Can not find a free port for use.");
      }
      return port.intValue();
   }

   private static Integer getFreePort(String host)
   {
      int p = getNextPort();

      if(checkPort(p, host))
      {
         return new Integer(p);
      }
      else
      {
         return null;
      }
   }

   private static synchronized int getNextPort()
   {
	  if (portCounter < MAX_LEGAL_PORT)
		  return portCounter++;
	  
	  portCounter = MIN_UNPRIVILEGED_PORT;
	  return MAX_LEGAL_PORT;
   }

   public static int getRandomStartingPort()
   {
      Object o = new Object();
      String os = o.toString();
      os = os.substring(17);
      int n = Integer.parseInt(os, 16);
      int p = Math.abs(new SecureRandom(String.valueOf(System.currentTimeMillis() + n).getBytes()).nextInt(2000)) + 2000;
      return p;
   }

}
