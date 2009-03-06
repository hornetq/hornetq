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
package org.jboss.test.messaging;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.util.ProxyAssertSupport;

/**
 * The base case for messaging tests.
 *
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.org">Tim Fox</a>
 * @author <a href="mailto:juha@jboss.org">Juha Lindfors</a>
 *
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class MessagingTestCase extends ProxyAssertSupport 
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      System.setProperty("java.naming.factory.initial", "org.jnp.interfaces.LocalOnlyContextFactory");
            //System.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
           
      String banner =
         "####################################################### Start " +
         (isRemote() ? "REMOTE" : "IN-VM") + " test: " + getName();

      log.info(banner);

      if (isRemote())
      {
         // log the test start in the remote log, this will make hunting through logs so much easier
         ServerManagement.log(ServerManagement.INFO, banner);
      }
   }

   protected void tearDown() throws Exception
   {
      String banner =
         "####################################################### Stop " + 
         (isRemote() ? "REMOTE" : "IN-VM") + " test: " + getName();

      log.info(banner);
      
      if (isRemote())
      {
         // log the test stop in the remote log, this will make hunting through logs so much easier
         ServerManagement.log(ServerManagement.INFO, banner);
      }
   }
   
   /**
    * @return true if this test is ran in "remote" mode, i.e. the server side of the test runs in a
    *         different VM than this one (that is running the client side)
    */
   protected boolean isRemote()
   {
      return ServerManagement.isRemote();
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
