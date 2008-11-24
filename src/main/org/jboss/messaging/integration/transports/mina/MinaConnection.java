/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.integration.transports.mina;

import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.future.IoFutureListener;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.ssl.SslFilter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * buhnaflagilibrn
 * @version <tt>$Revision$</tt>
 */
public class MinaConnection implements Connection
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MinaConnection.class);

   // Attributes ----------------------------------------------------

   private final IoSession session;

   private boolean closed;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public MinaConnection(final IoSession session)
   {
      this.session = session;
   }

   // Public --------------------------------------------------------

   // Connection implementation ----------------------------

   public synchronized void close()
   {
      if (closed)
      {
         return;
      }

      SslFilter sslFilter = (SslFilter) session.getFilterChain().get("ssl");

      if (session.getService() instanceof IoConnector) {
         if (sslFilter != null)
         {
            try
            {
               sslFilter.stopSsl(session).awaitUninterruptibly();
            }
            catch (Throwable t)
            {
               // ignore
            }
         }
         session.close().awaitUninterruptibly();
      } else {
         if (sslFilter != null)
         {
            try
            {
               sslFilter.stopSsl(session).addListener(IoFutureListener.CLOSE);
            }
            catch (Throwable t)
            {
               // ignore
            }
         } else {
            session.close();
         }
      }

      closed = true;
   }

   public MessagingBuffer createBuffer(int size)
   {
      IoBuffer buffer = IoBuffer.allocate(size);
      buffer.setAutoExpand(true);
      return new IoBufferWrapper(buffer);
   }

   public Object getID()
   {
      return Long.valueOf(session.getId());
   }

   public void write(final MessagingBuffer buffer)
   {
      session.write(buffer.getUnderlyingBuffer());
   }

   public String getRemoteAddress()
   {
      return session.getRemoteAddress().toString();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
