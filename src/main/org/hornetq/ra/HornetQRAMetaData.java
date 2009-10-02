/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.ra;

import javax.resource.ResourceException;
import javax.resource.spi.ManagedConnectionMetaData;

import org.hornetq.core.logging.Logger;

/**
 * Managed connection meta data
 *
 * @author <a href="mailto:adrian@jboss.com">Adrian Brock</a>
 * @author <a href="mailto:jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision:  $
 */
public class HornetQRAMetaData implements ManagedConnectionMetaData
{
   /** The logger */
   private static final Logger log = Logger.getLogger(HornetQRAMetaData.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The managed connection */
   private final HornetQRAManagedConnection mc;

   /**
    * Constructor
    * @param mc The managed connection
    */
   public HornetQRAMetaData(final HornetQRAManagedConnection mc)
   {
      if (trace)
      {
         log.trace("constructor(" + mc + ")");
      }

      this.mc = mc;
   }

   /**
    * Get the EIS product name
    * @return The name
    * @exception ResourceException Thrown if operation fails
    */
   public String getEISProductName() throws ResourceException
   {
      if (trace)
      {
         log.trace("getEISProductName()");
      }

      return "HornetQ";
   }

   /**
    * Get the EIS product version
    * @return The version
    * @exception ResourceException Thrown if operation fails
    */
   public String getEISProductVersion() throws ResourceException
   {
      if (trace)
      {
         log.trace("getEISProductVersion()");
      }

      return "2.0";
   }

   /**
    * Get the user name
    * @return The user name
    * @exception ResourceException Thrown if operation fails
    */
   public String getUserName() throws ResourceException
   {
      if (trace)
      {
         log.trace("getUserName()");
      }

      return mc.getUserName();
   }

  /**
    * Get the maximum number of connections -- RETURNS 0
    * @return The number
    * @exception ResourceException Thrown if operation fails
    */
   public int getMaxConnections() throws ResourceException
   {
      if (trace)
      {
         log.trace("getMaxConnections()");
      }

      return 0;
   }

}
