package org.hornetq.stomp.tests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HornetQTestServer
{

   public HornetQTestServer(int serverIndex)
   {
      
   }

   public void start() throws IOException
   {
      String[] cmdArray = assembleCommandArray();
      String[] envp = assembleEnvp();
      File dir = getWorkingDir();
      
      Process p = Runtime.getRuntime().exec(cmdArray, envp, dir);
   }

   private String[] assembleCommandArray()
   {
      //java
      String javaHome = System.getProperty("java.home");
      String javaPath = javaHome + File.separator + "bin" + File.separator + "java";
      
      List<String> fullCommand = new ArrayList<String>();
      fullCommand.add("\"" + javaPath + "\"");
      
      //classpath
      fullCommand.add("-cp");
      //hornetq jars
      fullCommand.addAll(getHornetQServerRuntimeJars());
      fullCommand.addAll(getTestClassBuildDirs());
      
      
      return null;
   }

   private Collection<? extends String> getHornetQServerRuntimeJars()
   {
      // TODO Auto-generated method stub
      return null;
   }

   private String[] assembleEnvp()
   {
      // TODO Auto-generated method stub
      return null;
   }

   private File getWorkingDir()
   {
      // TODO Auto-generated method stub
      return null;
   }

   public void shutdown()
   {
      // TODO Auto-generated method stub
      
   }

}
