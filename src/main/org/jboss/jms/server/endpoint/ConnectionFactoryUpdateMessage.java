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

package org.jboss.jms.server.endpoint;

import org.jboss.jms.client.delegate.ClientConnectionFactoryDelegate;
import java.util.Map;
import java.io.Serializable;

/**
 * This class holds the update cluster view sent by the server to client-side clustered connection
 * factories.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionFactoryUpdateMessage implements Serializable
{

   // Constants ------------------------------------------------------------------------------------

   static final long serialVersionUID = 7978093036163402989L;

   // Attributes -----------------------------------------------------------------------------------

   private ClientConnectionFactoryDelegate[] delegates;
   private Map failoverMap;

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public ConnectionFactoryUpdateMessage(ClientConnectionFactoryDelegate[] delegates,
                                         Map failoverMap)
   {
      this.delegates = delegates;
      this.failoverMap = failoverMap;
   }

   // Public ---------------------------------------------------------------------------------------

   public ClientConnectionFactoryDelegate[] getDelegates()
   {
      return delegates;
   }

   public void setDelegates(ClientConnectionFactoryDelegate[] delegates)
   {
      this.delegates = delegates;
   }

   public Map getFailoverMap()
   {
      return failoverMap;
   }

   public void setFailoverMap(Map failoverMap)
   {
      this.failoverMap = failoverMap;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("ConnectionFactoryUpdateMessage[");

      for(int i = 0; i < delegates.length; i++)
      {
         sb.append(delegates[i]);
         if (i < delegates.length - 1)
         {
            sb.append(',');
         }
      }

      sb.append("]");

      return sb.toString();
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
