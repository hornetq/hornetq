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
package org.jboss.jms.perf.framework.remoting.jbossremoting;

import org.jboss.remoting.Client;
import org.jboss.remoting.InvokerLocator;
import org.jboss.jms.perf.framework.protocol.KillRequest;
import org.jboss.logging.Logger;

/**
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version $Revision$
 *
 * $Id$
 */
public class ExecutorKiller
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ExecutorKiller.class);

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      try
      {
         new ExecutorKiller().run(Integer.valueOf(args[0]).intValue(), args[1]);
      }
      catch(Throwable t)
      {
         log.error("Killing failed", t);
      }
   }

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void run(int port, String host) throws Throwable
   {
      InvokerLocator locator = new InvokerLocator("socket://" + host + ":" + port);
      Client client = new Client(locator, "executor");
      client.invoke(new KillRequest());
      log.info("kill request has been sent to " + locator.getLocatorURI());
   }

   // Inner classes -------------------------------------------------

}
