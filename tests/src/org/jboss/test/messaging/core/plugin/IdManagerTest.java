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
package org.jboss.test.messaging.core.plugin;

import org.jboss.jms.delegate.IDBlock;
import org.jboss.messaging.core.plugin.IDManager;
import org.jboss.messaging.core.plugin.JDBCPersistenceManager;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.jmx.ServiceContainer;

/**
 * 
 * A IdManagerTest.
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class IdManagerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ServiceContainer sc;

   protected PersistenceManager pm;
   
   // Constructors --------------------------------------------------

   public IdManagerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      sc = new ServiceContainer("all");
      sc.start();                
      
      pm =
         new JDBCPersistenceManager(sc.getDataSource(), sc.getTransactionManager(),
                  sc.getPersistenceManagerSQLProperties(),
                  true, true, true, false, 100);     
      pm.start();
      
      pm.start();
            
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {      
      if (!ServerManagement.isRemote())
      {
         sc.stop();
         sc = null;
      }
      pm.stop();
      super.tearDown();
   }
   
   public void test1() throws Exception
   {
      IDManager idm = new IDManager("test_counter", 1000, pm);
      idm.start();
      
      int blockSize = 37;
            
      long nextLow = Long.MIN_VALUE;
      
      for (int i = 0; i < 1000; i++)
      {
         IDBlock block = idm.getIDBlock(blockSize);
                   
         assertTrue(block.getLow() >= nextLow);
         
         assertEquals(blockSize, 1 + block.getHigh() - block.getLow());
         
         nextLow = block.getHigh() + 1;         
      }
      
      idm.stop();
   }
   
   public void test2() throws Exception
   {
      IDManager idm = new IDManager("test_counter2", 100, pm);
      idm.start();
         
      for (int i = 0; i < 1000; i++)
      {
         long id = idm.getID();
         
         assertEquals(i, id);
      }
      
      idm.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}

