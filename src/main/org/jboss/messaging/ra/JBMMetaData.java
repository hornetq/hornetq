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
import javax.resource.spi.ManagedConnectionMetaData;

import org.jboss.messaging.core.logging.Logger;

/**
 * Managed connection meta data
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class JBMMetaData implements ManagedConnectionMetaData
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMMetaData.class);
   
   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The managed connection */
   private JBMManagedConnection mc;
   
   /**
    * Constructor
    * @param mc The managed connection
    */
   public JBMMetaData(JBMManagedConnection mc) {
      if (trace)
         log.trace("constructor(" + mc + ")");

      this.mc = mc;
   }
   
   /**
    * Get the EIS product name
    * @return The name
    * @exception ResourceException Thrown if operation fails
    */
   public String getEISProductName() throws ResourceException {
      if (trace)
         log.trace("getEISProductName()");
      
      return "JBoss Messaging";
   }

   /**
    * Get the EIS product version
    * @return The version
    * @exception ResourceException Thrown if operation fails
    */
   public String getEISProductVersion() throws ResourceException {
      if (trace)
         log.trace("getEISProductVersion()");

      return "2.0";
   }

   /**
    * Get the maximum number of connections -- RETURNS 0
    * @return The number
    * @exception ResourceException Thrown if operation fails
    */
   public int getMaxConnections() throws ResourceException {
      if (trace)
         log.trace("getMaxConnections()");

      return 0;
   }
    
   /**
    * Get the user name
    * @return The user name
    * @exception ResourceException Thrown if operation fails
    */
   public String getUserName() throws ResourceException {
      if (trace)
         log.trace("getUserName()");

      return mc.getUserName();
   }
}
