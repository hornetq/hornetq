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

package org.jboss.jms.wireformat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.jboss.jms.delegate.TopologyResult;

/**
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *          $Id$
 */
public class ConnectionFactoryGetTopologyResponse extends ResponseSupport
{

   // Constants ------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   TopologyResult result;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryGetTopologyResponse(TopologyResult result)
   {
      super(RESP_CONNECTIONFACTORY_GETTOPOLOGY);
      this.result = result;
   }

   public ConnectionFactoryGetTopologyResponse()
   {
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

   public void read(DataInputStream is) throws Exception
   {
      result = new TopologyResult();
      result.read(is);
   }


   public void write(DataOutputStream os) throws Exception
   {
      super.write(os);
      result.write(os);
      os.flush();
   }

   public Object getResponse()
   {
      return result;
   }
}
