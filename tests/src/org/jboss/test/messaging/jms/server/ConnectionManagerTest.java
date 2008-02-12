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
package org.jboss.test.messaging.jms.server;

import javax.jms.Connection;

import org.jboss.test.messaging.JBMServerTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class ConnectionManagerTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionManagerTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------
   
   public void testTwoConnectionsFromTheSameVM() throws Exception
   {
      
      assertActiveConnectionsOnTheServer(0);
      
      Connection conn_1 = getConnectionFactory().createConnection();
      
      assertActiveConnectionsOnTheServer(1);
      
      Connection conn_2 = getConnectionFactory().createConnection();
      
      assertActiveConnectionsOnTheServer(2);
      
      assertFalse(conn_1.equals(conn_2));
      
      conn_1.close();
      
      assertActiveConnectionsOnTheServer(1);
      
      conn_2.close();
      
      assertActiveConnectionsOnTheServer(0);
   }

}
