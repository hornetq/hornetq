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

package org.jboss.messaging.tests.unit.core.deployers.impl;

import java.io.File;
import java.net.URL;

import org.easymock.classextension.EasyMock;
import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.impl.FileDeploymentManager;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A FileDeploymentManagerTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class FileDeploymentManagerTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FileDeploymentManagerTest.class);
      
   public void testStartStop() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);
      
      Deployer deployer = EasyMock.createStrictMock(Deployer.class);
      
      try
      {
         fdm.registerDeployer(deployer);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      try
      {
         fdm.unregisterDeployer(deployer);
         fail("Should throw exception");
      }
      catch (IllegalStateException e)
      {
         //Ok
      }
      
      fdm.start();
      
      String filename = "fdm_test_file.xml";
      
      log.debug("Filename is " + filename);
      
      File file = new File("tests/tmpfiles/" + filename);
      
      log.debug(file.getAbsoluteFile());
      
      file.createNewFile();

      try
      {      
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename);
         
         URL url = file.toURL();
         
         deployer.deploy(url);
         
         EasyMock.replay(deployer);
         
         fdm.registerDeployer(deployer);
         
         EasyMock.verify(deployer);
         
         //Start again should do nothing
         
         EasyMock.reset(deployer);
         
         EasyMock.replay(deployer);
         
         fdm.start();
         
         EasyMock.verify(deployer);
         
         fdm.stop();
         
         try
         {
            fdm.registerDeployer(deployer);
            fail("Should throw exception");
         }
         catch (IllegalStateException e)
         {
            //Ok
         }
         
         try
         {
            fdm.unregisterDeployer(deployer);
            fail("Should throw exception");
         }
         catch (IllegalStateException e)
         {
            //Ok
         }
      }
      finally
      {
         file.delete();
      }      
   }
   
   public void testRegisterUnregister() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);
      
      Deployer deployer1 = EasyMock.createStrictMock(Deployer.class);
      Deployer deployer2 = EasyMock.createStrictMock(Deployer.class);
      Deployer deployer3 = EasyMock.createStrictMock(Deployer.class);
      
      fdm.start();
      
      String filename1 = "fdm_test_file.xml1";
      String filename2 = "fdm_test_file.xml2";
      String filename3 = "fdm_test_file.xml3";
      
      File file1 = new File("tests/tmpfiles/" + filename1);
      File file2 = new File("tests/tmpfiles/" + filename2);
      File file3 = new File("tests/tmpfiles/" + filename3);
       
      file1.createNewFile();
      file2.createNewFile();
      file3.createNewFile();
      
      try
      {      
         EasyMock.expect(deployer1.getConfigFileName()).andReturn(filename1);      
         URL url1 = file1.toURL();      
         deployer1.deploy(url1);
         
         EasyMock.expect(deployer2.getConfigFileName()).andReturn(filename2);      
         URL url2 = file2.toURL();      
         deployer2.deploy(url2);
         
         EasyMock.expect(deployer3.getConfigFileName()).andReturn(filename3);      
         URL url3 = file3.toURL();      
         deployer3.deploy(url3);
   
         EasyMock.replay(deployer1, deployer2, deployer3);
         
         fdm.registerDeployer(deployer1);
         fdm.registerDeployer(deployer2);
         fdm.registerDeployer(deployer3);
         
         EasyMock.verify(deployer1, deployer2, deployer3);
         
         assertEquals(3, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer1));
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertEquals(3, fdm.getDeployed().size());
         
         //Registering same again should do nothing
         
         EasyMock.reset(deployer1, deployer2, deployer3);
         EasyMock.replay(deployer1, deployer2, deployer3);
         
         fdm.registerDeployer(deployer1);
         
         EasyMock.verify(deployer1, deployer2, deployer3);
         
         assertEquals(3, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer1));
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertEquals(3, fdm.getDeployed().size());
         
         EasyMock.reset(deployer1, deployer2, deployer3);
         
         EasyMock.expect(deployer1.getConfigFileName()).andReturn(filename1); 
                  
         EasyMock.replay(deployer1, deployer2, deployer3);
         
         fdm.unregisterDeployer(deployer1);
                  
         EasyMock.verify(deployer1, deployer2, deployer3);
         
         assertEquals(2, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertEquals(2, fdm.getDeployed().size());
         
         EasyMock.reset(deployer1, deployer2, deployer3);
         
         EasyMock.expect(deployer2.getConfigFileName()).andReturn(filename2); 
         EasyMock.expect(deployer3.getConfigFileName()).andReturn(filename3); 
         
         EasyMock.replay(deployer1, deployer2, deployer3);
         
         fdm.unregisterDeployer(deployer2);
         fdm.unregisterDeployer(deployer3);
                  
         EasyMock.verify(deployer1, deployer2, deployer3);
         
         assertEquals(0, fdm.getDeployers().size());  
         assertEquals(0, fdm.getDeployed().size());
         
         //Now unregister again - should do nothing
         
         EasyMock.reset(deployer1, deployer2, deployer3);
         EasyMock.replay(deployer1, deployer2, deployer3);
         fdm.unregisterDeployer(deployer1);
         EasyMock.verify(deployer1, deployer2, deployer3);
         
         assertEquals(0, fdm.getDeployers().size());  
         assertEquals(0, fdm.getDeployed().size());         
      }
      finally
      {
         file1.delete();
         file2.delete();
         file3.delete();
      }
   }
   
   public void testRedeploy() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);
      
      Deployer deployer = EasyMock.createStrictMock(Deployer.class);
   
      fdm.start();
      
      String filename = "fdm_test_file.xml1";
  
      File file = new File("tests/tmpfiles/" + filename);
   
      file.createNewFile();
      long oldLastModified = file.lastModified();
 
      try
      {      
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename);      
         URL url = file.toURL();      
         deployer.deploy(url);
         
         EasyMock.replay(deployer);
         
         fdm.registerDeployer(deployer);

         EasyMock.verify(deployer);
         
         //Touch the file
         file.setLastModified(oldLastModified + 1000);
         
         EasyMock.reset(deployer);
                           
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename);          
         deployer.redeploy(url);
         
         EasyMock.replay(deployer);
         
         fdm.run();
         
         EasyMock.verify(deployer);
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(1, fdm.getDeployed().size());
         
         assertEquals(oldLastModified + 1000, fdm.getDeployed().get(url).lastModified);
          
         //Scanning again should not redeploy
         
         EasyMock.reset(deployer);
         
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename); 
         
         EasyMock.replay(deployer);
         
         fdm.run();
         
         EasyMock.verify(deployer);
         
         assertEquals(oldLastModified + 1000, fdm.getDeployed().get(url).lastModified);
      }
      finally
      {
         file.delete();
      }
   }
   
   public void testUndeployAndDeployAgain() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);
      
      Deployer deployer = EasyMock.createStrictMock(Deployer.class);
   
      fdm.start();
      
      String filename = "fdm_test_file.xml1";
  
      File file = new File("tests/tmpfiles/" + filename);
   
      file.createNewFile();
 
      try
      {      
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename);      
         URL url = file.toURL();      
         deployer.deploy(url);
         
         EasyMock.replay(deployer);
         
         fdm.registerDeployer(deployer);

         EasyMock.verify(deployer);
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(1, fdm.getDeployed().size());
         
         file.delete();
         
         //This should cause undeployment
         
         EasyMock.reset(deployer);
         
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename);   
         
         deployer.undeploy(url);
         
         EasyMock.replay(deployer);
         
         fdm.run();
         
         EasyMock.verify(deployer);
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(0, fdm.getDeployed().size());
         
         //Recreate file and it should be redeployed
         
         file.createNewFile();
         
         EasyMock.reset(deployer);
         
         EasyMock.expect(deployer.getConfigFileName()).andReturn(filename);  
         
         deployer.deploy(url);
         
         EasyMock.replay(deployer);
         
         fdm.run();
         
         EasyMock.verify(deployer);
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(1, fdm.getDeployed().size());
      }
      finally
      {
         file.delete();
      }
   }
}
