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

package org.jboss.jms.server.remoting;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

import org.jboss.jms.client.remoting.ClientSocketWrapper;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:tom.elrod@jboss.com">Tom Elrod</a>
 * @author <a href="mailto:tom.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class ServerSocketWrapper extends ClientSocketWrapper
{
   // Constants -----------------------------------------------------

   final static private Logger log = Logger.getLogger(ServerSocketWrapper.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // Constructors --------------------------------------------------

   public ServerSocketWrapper(Socket socket) throws Exception
   {
      super(socket);
   }

   public ServerSocketWrapper(Socket socket, Map metadata, Integer timeout) throws Exception
   {
      super(socket, metadata, timeout);
   }
   
   // SocketWrapper overrides ----------------------------------------------------------------------

   public void close() throws IOException
   {
      if(getSocket() != null)
      {
         try
         {
            getOutputStream().write(CLOSING);
            getOutputStream().flush();
            log.debug("wrote CLOSING byte");
         }
         catch (IOException e)
         {
            log.debug("cannot write CLOSING byte", e);
         }
         super.close();
      }
   }

   // Public --------------------------------------------------------

   public void checkConnection() throws IOException
   {
      // Perform acknowledgement to convince client that the socket is still active
      byte ACK = 0;

      try
      {
         ACK = ((DataInputStream)getInputStream()).readByte();
      }
      catch(EOFException eof)
      {
         if (trace)
         {
            log.trace("socket timeout is set to: " + getTimeout());
            log.trace("EOFException waiting on ACK in readByte().");
         }
         throw eof;
      }
      catch(IOException e)
      {
         log.trace("IOException when reading in ACK", e);
         throw e;
      }

      if (trace) { log.trace("acknowledge read byte " + Thread.currentThread()); }

      DataOutputStream out = (DataOutputStream)getOutputStream();
      out.writeByte(ACK);
      out.flush();
   }

   public String toString()
   {
      Socket socket = getSocket();
      return "ServerSocketWrapper[" + socket + "." + 
         Integer.toHexString(System.identityHashCode(socket)) + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
