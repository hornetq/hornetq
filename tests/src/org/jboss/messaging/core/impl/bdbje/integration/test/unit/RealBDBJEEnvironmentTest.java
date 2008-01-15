package org.jboss.messaging.core.impl.bdbje.integration.test.unit;

import java.io.File;
import java.io.FileOutputStream;

import org.jboss.messaging.core.impl.bdbje.BDBJEEnvironment;
import org.jboss.messaging.core.impl.bdbje.integration.RealBDBJEEnvironment;
import org.jboss.messaging.core.impl.bdbje.test.unit.BDBJEEnvironmentTestBase;

/**
 * 
 * A RealBDBJEEnvironmentTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class RealBDBJEEnvironmentTest extends BDBJEEnvironmentTestBase
{
   protected void setUp() throws Exception
   {   
      createDir(ENV_DIR);
      
      env = createEnvironment();
      
      env.setEnvironmentPath(ENV_DIR);
      
      env.setCreateEnvironment(true);
      
      env.start();
      
      database = env.getDatabase("test-db");            
   }
   
   protected BDBJEEnvironment createEnvironment() throws Exception
   {
      BDBJEEnvironment env = new RealBDBJEEnvironment(true);
      
      env.setTransacted(true);
      
      return env;
   }    
   
   protected void createDir(String path)
   {  
      File file = new File(path);
      
      deleteDirectory(file);
      
      file.mkdir();
   }
   
   protected void copyEnvironment() throws Exception
   {
      File envDir = new File(ENV_DIR);
      
      File envCopyDir = new File(ENV_COPY_DIR);
      
      deleteDirectory(envCopyDir);
      
      copyRecursive(envDir, envCopyDir);
      
      deleteDirectory(envDir);
      
      //Need to sync
      
      FileOutputStream fos = new FileOutputStream(ENV_COPY_DIR + "/je.lck");
      
      fos.getFD().sync();
   }
   
   protected void copyBackEnvironment() throws Exception
   {
      File envDir = new File(ENV_DIR);
      
      File envCopyDir = new File(ENV_COPY_DIR);
      
      deleteDirectory(envDir);
      
      copyRecursive(envCopyDir, envDir);
      
      deleteDirectory(envCopyDir);
      
      //Need to sync
      
      FileOutputStream fos = new FileOutputStream(ENV_DIR + "/je.lck");
      
      fos.getFD().sync();
   }    
   
   protected boolean isFake()
   {
      return false;
   }
}
