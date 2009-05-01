/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ConnectionRequestInfo;
import javax.resource.spi.ManagedConnection;
import javax.resource.spi.ManagedConnectionFactory;

import org.jboss.messaging.core.logging.Logger;

/**
 * The connection manager used in non-managed environments.
 * 
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMConnectionManager implements ConnectionManager
{
   /** Serial version UID */
   static final long serialVersionUID = 4409118162975011014L;

   /** The logger */
   private static final Logger log = Logger.getLogger(JBMConnectionManager.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /**
    * Constructor
    */
   public JBMConnectionManager()
   {
      if (trace)
      {
         log.trace("constructor()");
      }
   }

   /**
    * Allocates a connection
    * @param mcf The managed connection factory
    * @param cxRequestInfo The connection request information 
    * @return The connection
    * @exception ResourceException Thrown if there is a problem obtaining the connection
    */
   public Object allocateConnection(final ManagedConnectionFactory mcf, final ConnectionRequestInfo cxRequestInfo) throws ResourceException
   {
      if (trace)
      {
         log.trace("allocateConnection(" + mcf + ", " + cxRequestInfo + ")");
      }

      ManagedConnection mc = mcf.createManagedConnection(null, cxRequestInfo);
      Object c = mc.getConnection(null, cxRequestInfo);

      if (trace)
      {
         log.trace("Allocated connection: " + c + ", with managed connection: " + mc);
      }

      return c;
   }
}
