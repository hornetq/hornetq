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

package org.jboss.jms.client.remoting;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.remoting.transport.socket.OpenConnectionChecker;
import org.jboss.remoting.transport.socket.SocketWrapper;

/**
 * @author <a href="mailto:tom.elrod@jboss.com">Tom Elrod</a>
 * @author <a href="mailto:tom.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class ClientSocketWrapper extends SocketWrapper implements OpenConnectionChecker
{
   // Constants ------------------------------------------------------------------------------------
   final static private Logger log = Logger.getLogger(ClientSocketWrapper.class);
   final static protected int CLOSING = 1;
   
   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private DataInputStream in;
   private DataOutputStream out;

   // Constructors ---------------------------------------------------------------------------------

   public ClientSocketWrapper(Socket socket) throws IOException
   {
      super(socket);
      createStreams(socket, null);
   }

   public ClientSocketWrapper(Socket socket, Map metadata, Integer timeout) throws Exception
   {
      super(socket, timeout);
      createStreams(socket, metadata);
   }

   // SocketWrapper overrides ----------------------------------------------------------------------

   public OutputStream getOutputStream()
   {
      return out;
   }

   public InputStream getInputStream()
   {
      return in;
   }

   public void checkConnection() throws IOException
   {
      // Test to see if socket is alive by send ACK message
      final byte ACK = 1;

      out.writeByte(ACK);
      out.flush();
      in.readByte();
   }
   // OpenConnectionChecker implementation ---------------------------------------------------------

   public void checkOpenConnection() throws IOException
   {
      if (in.available() > 0)
      {
         log.trace("remote endpoint has closed");
         throw new IOException("remote endpoint has closed");
      }
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      Socket socket = getSocket();
      return "ClientSocketWrapper[" + socket + "." +
         Integer.toHexString(System.identityHashCode(socket)) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void createStreams(Socket socket, Map metadata) throws IOException
   {
      out = createOutputStream(socket);
      in = createInputStream(socket);
   }

   protected DataInputStream createInputStream(Socket socket)
         throws IOException
   {
      BufferedInputStream bin = new BufferedInputStream(socket.getInputStream());
      
      return new DataInputStream(bin);
   }

   protected DataOutputStream createOutputStream(Socket socket)
         throws IOException
   {
      BufferedOutputStream bout = new BufferedOutputStream(socket.getOutputStream());
      
      return new DataOutputStream(bout);
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
