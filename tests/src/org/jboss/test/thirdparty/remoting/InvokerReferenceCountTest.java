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
package org.jboss.test.thirdparty.remoting;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class InvokerReferenceCountTest extends MessagingTestCase
{
   public InvokerReferenceCountTest(String name)
   {
      super(name);
   }

   private ConnectionFactory cf;
   
   protected void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start("all");
      
      ServerManagement.deployQueue("testQueue");
      
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      Queue queue = (Queue)ic.lookup("/queue/testQueue");
      
      ic.close();      
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      ServerManagement.undeployQueue("testQueue");
      
      ServerManagement.stop();
   }
   
   public void testReferenceCount() throws Exception
   {

      log.info("Creating conn1");
      Connection conn1 = cf.createConnection();              
      log.info("Created conn2");                     
      
      log.info("Creating conn2");
      Connection conn2 = cf.createConnection();
      log.info("Created conn2");   
      
      Thread.sleep(4000);
      
      log.info("Closing conn2");
      conn2.close();      
      log.info("Closed conn2");
                               
      log.info("Creating conn3");
      Connection conn3 = cf.createConnection();
      log.info("Created conn3");
      
      Thread.sleep(4000);
      
      log.info("Closing conn3");
      conn3.close();      
      log.info("Closed conn3");
            
      log.info("Closing conn1");
      conn1.close();            
      log.info("Closed conn1");
   }

}
