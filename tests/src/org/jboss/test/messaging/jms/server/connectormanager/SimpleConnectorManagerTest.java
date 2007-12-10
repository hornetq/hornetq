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
package org.jboss.test.messaging.jms.server.connectormanager;

import org.jboss.jms.server.connectormanager.SimpleConnectorManager;
import org.jboss.test.messaging.JBMBaseTestCase;

import javax.naming.InitialContext;

/**
 * 
 * A SimpleConnectorManagerTest

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class SimpleConnectorManagerTest extends JBMBaseTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   // Constructors --------------------------------------------------

   public SimpleConnectorManagerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testSimpleConnectorManager() throws Exception
   {
      SimpleConnectorManager cm = new SimpleConnectorManager();
      
      cm.registerConnector("connector1");      
      assertEquals(1, cm.getCount("connector1"));      
      assertTrue(cm.containsConnector("connector1"));
      
      cm.registerConnector("connector2");      
      assertEquals(1, cm.getCount("connector2"));      
      assertTrue(cm.containsConnector("connector2"));
      
      cm.registerConnector("connector1");      
      assertEquals(2, cm.getCount("connector1"));      
      assertTrue(cm.containsConnector("connector1"));
      
      cm.unregisterConnector("connector2");      
      assertEquals(0, cm.getCount("connector2"));      
      assertFalse(cm.containsConnector("connector2"));
      
      cm.unregisterConnector("connector1");      
      assertEquals(1, cm.getCount("connector1"));      
      assertTrue(cm.containsConnector("connector1"));
      
      cm.unregisterConnector("connector1");      
      assertEquals(0, cm.getCount("connector1"));      
      assertFalse(cm.containsConnector("connector1"));
        
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
 
}


