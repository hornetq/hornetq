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
package org.jboss.test.messaging.jms.bridge;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * 
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class BridgeTestBase extends MessagingTestCase
{
   private static final Logger log = Logger.getLogger(BridgeTest.class);
   
   private static final int NODE_COUNT = 2;
   
   public BridgeTestBase(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      
      if (ServerManagement.isRemote())
      {                 
         for (int i = 0; i < NODE_COUNT; i++)
         {
            // make sure all servers are created and started; make sure that database is zapped
            // ONLY for the first server, the others rely on values they expect to find in shared
            // tables; don't clear the database for those.
            ServerManagement.start(i, "all,-transaction,jbossjta", i == 0);
         }
      }

   }

   protected void tearDown() throws Exception
   { 
      if (ServerManagement.isRemote())
      {         
         for (int i = 0; i < NODE_COUNT; i++)
         {
            try
            {
               if (ServerManagement.isStarted(i))
               {
                  ServerManagement.log(ServerManagement.INFO, "Undeploying Server " + i, i);
                  
                  ServerManagement.stop(i);
               }
            }
            catch (Exception e)
            {
               log.error("Failed to stop server", e);
            }
         }
         
         for (int i = 1; i < NODE_COUNT; i++)
         {
            try
            {
               ServerManagement.kill(i);
            }
            catch (Exception e)
            {
               log.error("Failed to kill server", e);
            }
         }
      }
      
      super.tearDown();
      
   }
   
   
   // Inner classes -------------------------------------------------------------------
   
}

