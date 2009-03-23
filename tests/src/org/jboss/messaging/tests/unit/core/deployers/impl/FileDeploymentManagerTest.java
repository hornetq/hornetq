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

import org.jboss.messaging.core.deployers.Deployer;
import org.jboss.messaging.core.deployers.impl.FileDeploymentManager;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.util.UnitTestCase;

import java.io.File;
import java.net.URL;

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

      String filename = "fdm_test_file.xml";

      log.debug("Filename is " + filename);

      File file = new File("tests/tmpfiles/" + filename);

      log.debug(file.getAbsoluteFile());

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);
      
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


      try
      {
         fdm.registerDeployer(deployer);
         assertEquals(file.toURL(), deployer.deployedUrl);
         deployer.deployedUrl = null;
         fdm.start();
         assertNull(deployer.deployedUrl);
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


      FakeDeployer deployer1 = new FakeDeployer(filename1);
      FakeDeployer deployer2 = new FakeDeployer(filename2);
      FakeDeployer deployer3 = new FakeDeployer(filename3);
      try
      {
         URL url1 = file1.toURL();      
         deployer1.deploy(url1);

         URL url2 = file2.toURL();      
         deployer2.deploy(url2);

         URL url3 = file3.toURL();      
         deployer3.deploy(url3);
         
         fdm.registerDeployer(deployer1);
         fdm.registerDeployer(deployer2);
         fdm.registerDeployer(deployer3);

         
         assertEquals(3, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer1));
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertEquals(3, fdm.getDeployed().size());

         assertEquals(file1.toURL(), deployer1.deployedUrl);
         assertEquals(file2.toURL(), deployer2.deployedUrl);
         assertEquals(file3.toURL(), deployer3.deployedUrl);
         //Registering same again should do nothing

         
         fdm.registerDeployer(deployer1);

         
         assertEquals(3, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer1));
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertEquals(3, fdm.getDeployed().size());

         
         fdm.unregisterDeployer(deployer1);

         
         assertEquals(2, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertEquals(2, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer2);
         fdm.unregisterDeployer(deployer3);
         
         assertEquals(0, fdm.getDeployers().size());  
         assertEquals(0, fdm.getDeployed().size());
         
         //Now unregister again - should do nothing

         fdm.unregisterDeployer(deployer1);
         
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

   
      fdm.start();
      
      String filename = "fdm_test_file.xml1";
  
      File file = new File("tests/tmpfiles/" + filename);
   
      file.createNewFile();
      long oldLastModified = file.lastModified();

      FakeDeployer deployer = new FakeDeployer(filename);
      try
      {
         URL url = file.toURL();      
         deployer.deploy(url);
         
         fdm.registerDeployer(deployer);
         assertEquals(file.toURL(), deployer.deployedUrl);
         //Touch the file
         file.setLastModified(oldLastModified + 1000);

         deployer.redeploy(url);
         
         fdm.run();
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(1, fdm.getDeployed().size());
         assertEquals(file.toURL(), deployer.reDeployedUrl);
         assertEquals(oldLastModified + 1000, fdm.getDeployed().get(url).lastModified);
         deployer.reDeployedUrl = null; 
         //Scanning again should not redeploy
         
         fdm.run();
         
         assertEquals(oldLastModified + 1000, fdm.getDeployed().get(url).lastModified);
         assertNull(deployer.reDeployedUrl);
      }
      finally
      {
         file.delete();
      }
   }

   public void testUndeployAndDeployAgain() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

   
      fdm.start();
      
      String filename = "fdm_test_file.xml1";
  
      File file = new File("tests/tmpfiles/" + filename);
   
      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);
      try
      {
         URL url = file.toURL();      
         deployer.deploy(url);

         
         fdm.registerDeployer(deployer);
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(1, fdm.getDeployed().size());
         assertEquals(file.toURL(), deployer.deployedUrl);
         deployer.deployedUrl = null;
         file.delete();
         
         //This should cause undeployment

         deployer.undeploy(url);
         assertEquals(file.toURL(), deployer.unDeployedUrl);
         
         fdm.run();

         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(0, fdm.getDeployed().size());
         
         //Recreate file and it should be redeployed
         
         file.createNewFile();

         
         deployer.deploy(url);

         
         fdm.run();
         
         assertEquals(1, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer));
         assertEquals(1, fdm.getDeployed().size());

         assertEquals(file.toURL(), deployer.deployedUrl);
      }
      finally
      {
         file.delete();
      }
   }

   class FakeDeployer implements Deployer
   {
      URL deployedUrl;
      URL unDeployedUrl;
      URL reDeployedUrl;
      boolean started;
      private String file;

      public FakeDeployer(String file)
      {
         this.file = file;
      }

      public String[] getConfigFileNames()
      {
         return new String[]{file};
      }

      public void deploy(URL url) throws Exception
      {
         deployedUrl = url;
      }

      public void redeploy(URL url) throws Exception
      {
         reDeployedUrl = url;
      }

      public void undeploy(URL url) throws Exception
      {
         unDeployedUrl = url;
      }

      public void start() throws Exception
      {
         started = true;
      }

      public void stop() throws Exception
      {
         started = false;
      }

      public boolean isStarted()
      {
         return started;
      }
   }
}
