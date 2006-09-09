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
package org.jboss.test.messaging.core;

import java.io.Serializable;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.Routable;

/**
 * A simple test Filter that accepts a message based on the presence of a certain header value.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SimpleFilter implements Filter
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 4962761960390959612L;

   private static final Logger log = Logger.getLogger(SimpleFilter.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String headerName;
   protected Serializable headerValue;

   // Constructors --------------------------------------------------

   public SimpleFilter(String headerName, Serializable headerValue)
   {
      this.headerName = headerName;
      this.headerValue = headerValue;
   }

   // Filter implementation -----------------------------------------
   
   public String getFilterString()
   {
      return null;
   }

   public boolean accept(Routable r)
   {
      boolean accepted = false;
      if (r != null)
      {
         Object o = r.getHeader(headerName);
         if (o == null && headerValue == null)
         {
            accepted = false;
         }
         else
         {
            accepted = o.equals(headerValue);
         }
      }
      log.trace("the filter " + (accepted ? "accepted" : "rejected") + " the message " + r);
      return accepted;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
