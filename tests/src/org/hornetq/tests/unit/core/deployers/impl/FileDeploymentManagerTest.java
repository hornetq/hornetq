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

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.core.deployers.Deployer;
import org.hornetq.core.deployers.impl.FileDeploymentManager;
import org.hornetq.core.logging.Logger;
import org.hornetq.tests.util.UnitTestCase;

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
      testStartStop1("fdm_test_file.xml");
   }

   public void testStartStop2() throws Exception
   {
      testStartStop2("fdm_test_file.xml");
   }

   public void testStartStop1WithWhitespace() throws Exception
   {
      testStartStop1("fdm test file.xml");
      if (!isWindows())
      {
         testStartStop1("fdm\ttest\tfile.xml");
      }
   }

   public void testStartStop2WithWhitespace() throws Exception
   {
      testStartStop2("fdm test file.xml");
      if (!isWindows())
      {
         testStartStop2("fdm\ttest\tfile.xml");
      }
   }

   private void testStartStop1(final String filename) throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      FileDeploymentManagerTest.log.debug("Filename is " + filename);

      File file = new File("tests/tmpfiles/" + filename);

      FileDeploymentManagerTest.log.debug(file.getAbsoluteFile());

      System.out.println("========file name: " + file.getAbsolutePath());
      
      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);

      fdm.registerDeployer(deployer);

      fdm.unregisterDeployer(deployer);

      fdm.registerDeployer(deployer);

      fdm.start();
      try
      {
         URL expected = file.toURI().toURL();
         URL deployedUrl = deployer.deployedUrl;
         Assert.assertTrue(expected.toString().equalsIgnoreCase(deployedUrl.toString()));
         deployer.deployedUrl = null;
         fdm.start();
         Assert.assertNull(deployer.deployedUrl);
         fdm.stop();

      }
      finally
      {
         file.delete();
      }
   }

   private void testStartStop2(final String filename) throws Exception
   {
      FileDeploymentManager fdm = new FileDeploymentManager(Long.MAX_VALUE);

      FileDeploymentManagerTest.log.debug("Filename is " + filename);

      File file = new File("tests/tmpfiles/" + filename);

      FileDeploymentManagerTest.log.debug(file.getAbsoluteFile());

      file.createNewFile();

      FakeDeployer deployer = new FakeDeployer(filename);

      fdm.start();

      try
      {
         fdm.registerDeployer(deployer);
         URL expected = file.toURI().toURL();
         URL deployedUrl = deployer.deployedUrl;
         Assert.assertTrue(expected.toString().equalsIgnoreCase(deployedUrl.toString()));
         deployer.deployedUrl = null;
         fdm.start();
         Assert.assertNull(deployer.deployedUrl);
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
         URL url1 = file1.toURI().toURL();
         deployer1.deploy(url1);

         URL url2 = file2.toURI().toURL();
         deployer2.deploy(url2);

         URL url3 = file3.toURI().toURL();
         deployer3.deploy(url3);

         deployer4.deploy(url3);

         fdm.registerDeployer(deployer1);
         fdm.registerDeployer(deployer2);
         fdm.registerDeployer(deployer3);
         fdm.registerDeployer(deployer4);

         Assert.assertEquals(4, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer1));
         Assert.assertTrue(fdm.getDeployers().contains(deployer2));
         Assert.assertTrue(fdm.getDeployers().contains(deployer3));
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(4, fdm.getDeployed().size());

         Assert.assertEquals(file1.toURI().toURL(), deployer1.deployedUrl);
         Assert.assertEquals(file2.toURI().toURL(), deployer2.deployedUrl);
         Assert.assertEquals(file3.toURI().toURL(), deployer3.deployedUrl);
         Assert.assertEquals(file3.toURI().toURL(), deployer4.deployedUrl);
         // Registering same again should do nothing

         fdm.registerDeployer(deployer1);

         Assert.assertEquals(4, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer1));
         Assert.assertTrue(fdm.getDeployers().contains(deployer2));
         Assert.assertTrue(fdm.getDeployers().contains(deployer3));
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(4, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer1);

         Assert.assertEquals(3, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer2));
         Assert.assertTrue(fdm.getDeployers().contains(deployer3));
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(3, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer2);
         fdm.unregisterDeployer(deployer3);

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer4));
         Assert.assertEquals(1, fdm.getDeployed().size());

         fdm.unregisterDeployer(deployer4);

         Assert.assertEquals(0, fdm.getDeployers().size());
         Assert.assertEquals(0, fdm.getDeployed().size());

         // Now unregister again - should do nothing

         fdm.unregisterDeployer(deployer1);

         Assert.assertEquals(0, fdm.getDeployers().size());
         Assert.assertEquals(0, fdm.getDeployed().size());
      }
      finally
      {
         file1.delete();
         file2.delete();
         file3.delete();
      }
      
      fdm.stop();
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
         URL url = file.toURI().toURL();
         deployer.deploy(url);

         fdm.registerDeployer(deployer);
         Assert.assertEquals(file.toURI().toURL(), deployer.deployedUrl);
         // Touch the file
         file.setLastModified(oldLastModified + 1000);

         deployer.redeploy(url);

         fdm.run();

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(1, fdm.getDeployed().size());
         URL expected = file.toURI().toURL();
         URL deployedUrl = deployer.deployedUrl;
         Assert.assertTrue(expected.toString().equalsIgnoreCase(deployedUrl.toString()));
         Pair<URL, Deployer> pair = new Pair<URL, Deployer>(url, deployer);
         Assert.assertEquals(oldLastModified + 1000, fdm.getDeployed().get(pair).lastModified);
         deployer.reDeployedUrl = null;
         // Scanning again should not redeploy

         fdm.run();

         Assert.assertEquals(oldLastModified + 1000, fdm.getDeployed().get(pair).lastModified);
         Assert.assertNull(deployer.reDeployedUrl);
      }
      finally
      {
         file.delete();
         fdm.stop();
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
         URL url = file.toURI().toURL();
         deployer.deploy(url);

         fdm.registerDeployer(deployer);

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(1, fdm.getDeployed().size());
         Assert.assertEquals(file.toURI().toURL(), deployer.deployedUrl);
         deployer.deployedUrl = null;
         file.delete();

         // This should cause undeployment

         deployer.undeploy(url);
         Assert.assertEquals(file.toURI().toURL(), deployer.unDeployedUrl);

         fdm.run();

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(0, fdm.getDeployed().size());

         // Recreate file and it should be redeployed

         file.createNewFile();

         deployer.deploy(url);

         fdm.run();

         Assert.assertEquals(1, fdm.getDeployers().size());
         Assert.assertTrue(fdm.getDeployers().contains(deployer));
         Assert.assertEquals(1, fdm.getDeployed().size());

         Assert.assertEquals(file.toURI().toURL(), deployer.deployedUrl);
      }
      finally
      {
         file.delete();
         fdm.stop();
      }
   }

   class FakeDeployer implements Deployer
   {
      URL deployedUrl;

      URL unDeployedUrl;

      URL reDeployedUrl;

      boolean started;

      private final String file;

      public FakeDeployer(final String file)
      {
         this.file = file;
      }

      public String[] getConfigFileNames()
      {
         return new String[] { file };
      }

      public void deploy(final URL url) throws Exception
      {
         deployedUrl = url;
      }

      public void redeploy(final URL url) throws Exception
      {
         reDeployedUrl = url;
      }

      public void undeploy(final URL url) throws Exception
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
