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
package org.jboss.jms.perf.framework.protocol;

import org.jboss.jms.perf.framework.remoting.Request;
import org.jboss.jms.perf.framework.remoting.Result;
import org.jboss.jms.perf.framework.remoting.SimpleResult;
import org.jboss.jms.perf.framework.remoting.Context;
import org.jboss.logging.Logger;

/**
 * @author <a href="ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ResetRequest implements Request
{
  // Constants -----------------------------------------------------

   private transient static final Logger log = Logger.getLogger(ResetRequest.class);

  private static final long serialVersionUID = -349573297432452357L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Request implementation ----------------------------------------

   public Result execute(Context context) throws Exception
   {
      log.info("reset request executed");
      return new SimpleResult();
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "RESET REQUEST";
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
