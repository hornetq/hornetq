/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */ 

package org.hornetq.tests.unit.core.deployers.impl;

import java.io.File;
import java.net.URL;

import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.Pair;

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
      
   public void testStartStop1() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      String filename = "fdm_test_file.xml";

      log.debug("Filename is " + filename);

      File file = new File("tests/tmpfiles/" + filename);

      log.debug(file.getAbsoluteFile());

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);
      
      fdm.registerDeployer(deployer);
      
      fdm.unregisterDeployer(deployer);
      
      fdm.registerDeployer(deployer);
                 
      fdm.start();
      try
      {        
         assertEquals(file.toURL(), deployer.deployedUrl);
         deployer.deployedUrl = null;
         fdm.start();
         assertNull(deployer.deployedUrl);
         fdm.stop();
         
      }
      finally
      {
         file.delete();
      }      
   }
   
   public void testStartStop2() throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      String filename = "fdm_test_file.xml";

      log.debug("Filename is " + filename);

      File file = new File("tests/tmpfiles/" + filename);

      log.debug(file.getAbsoluteFile());

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);
      
      fdm.start();

      try
      {
         fdm.registerDeployer(deployer);
         assertEquals(file.toURL(), deployer.deployedUrl);
         deployer.deployedUrl = null;
         fdm.start();
         assertNull(deployer.deployedUrl);
         fdm.stop();         
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
      FakeDeployer deployer4 = new FakeDeployer(filename3); // Can have multiple deployers on the same file
      try
      {
         URL url1 = file1.toURL();      
         deployer1.deploy(url1);

         URL url2 = file2.toURL();      
         deployer2.deploy(url2);

         URL url3 = file3.toURL();      
         deployer3.deploy(url3);
         
         deployer4.deploy(url3);
         
         fdm.registerDeployer(deployer1);
         fdm.registerDeployer(deployer2);
         fdm.registerDeployer(deployer3);
         fdm.registerDeployer(deployer4);

         
         assertEquals(4, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer1));
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertTrue(fdm.getDeployers().contains(deployer4));
         assertEquals(4, fdm.getDeployed().size());

         assertEquals(file1.toURL(), deployer1.deployedUrl);
         assertEquals(file2.toURL(), deployer2.deployedUrl);
         assertEquals(file3.toURL(), deployer3.deployedUrl);
         assertEquals(file3.toURL(), deployer4.deployedUrl);
         //Registering same again should do nothing

         
         fdm.registerDeployer(deployer1);

         
         assertEquals(4, fdm.getDeployers().size());
         assertTrue(fdm.getDeployers().contains(deployer1));
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertTrue(fdm.getDeployers().contains(deployer4));
         assertEquals(4, fdm.getDeployed().size());

         
         fdm.unregisterDeployer(deployer1);

         
         assertEquals(3, fdm.getDeployers().size());        
         assertTrue(fdm.getDeployers().contains(deployer2));
         assertTrue(fdm.getDeployers().contains(deployer3));
         assertTrue(fdm.getDeployers().contains(deployer4));
         assertEquals(3, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer2);
         fdm.unregisterDeployer(deployer3);
         
         assertEquals(1, fdm.getDeployers().size());        
         assertTrue(fdm.getDeployers().contains(deployer4));
         assertEquals(1, fdm.getDeployed().size());
         
         fdm.unregisterDeployer(deployer4);
         
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
         Pair<URL, Deployer> pair = new Pair<URL, Deployer>(url, deployer);
         assertEquals(oldLastModified + 1000, fdm.getDeployed().get(pair).lastModified);
         deployer.reDeployedUrl = null; 
         //Scanning again should not redeploy
         
         fdm.run();
         
         assertEquals(oldLastModified + 1000, fdm.getDeployed().get(pair).lastModified);
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
