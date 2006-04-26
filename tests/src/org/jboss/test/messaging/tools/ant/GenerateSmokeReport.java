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
package org.jboss.test.messaging.tools.ant;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;


/**
 * Generates a HTML smoke test report based on raw smoke run data.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class GenerateSmokeReport
{
   // Constants -----------------------------------------------------

   public static final String DEFAULT_OUTPUT_BASENAME="smoke-test-report";

   // Static --------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      new GenerateSmokeReport(args).run();
   }

   // Attributes ----------------------------------------------------

   private File inputFile;
   private File outputDir;
   private String outputFileName;
   private File installerDir;

   // Constructors --------------------------------------------------

   private GenerateSmokeReport(String[] args) throws Exception
   {
      String baseName = null;

      for(int i = 0; i < args.length; i++)
      {
         if ("-inputfile".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("File name must follow -inputfile");
            }
            inputFile = new File(args[++i]);
         }
         else if ("-outputdir".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("Output directory name must follow -outputdir");
            }
            outputDir = new File(args[++i]);
         }
         else if ("-basename".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("Output file name must follow -name");
            }
            baseName = args[++i];
         }
         else if ("-installerdir".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("Installer directory must follow -installerdir");
            }
            installerDir = new File(args[++i]);
         }
         else
         {
            throw new Exception("Unknown argument: " + args[i]);
         }
      }

      if (inputFile == null)
      {
         throw new Exception("No input file specified");
      }

      if (!inputFile.canRead())
      {
         throw new Exception("The input file " + inputFile + " does not exist or cannot be read");
      }

      if (outputDir == null)
      {
         // no output directory specified, using the current directory
         outputDir = new File(".");
      }

      if (!outputDir.canWrite())
      {
         throw new Exception("The output directory " + outputDir + " is not writable");
      }

      if (baseName == null)
      {
         baseName = DEFAULT_OUTPUT_BASENAME;
      }

      outputFileName = baseName + ".java-" + System.getProperty("java.version") + ".html";
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void run() throws Exception
   {
      ReportData d = parseInputFile(inputFile);
      generateReport(d);
   }

   private ReportData parseInputFile(File f) throws Exception
   {
      BufferedReader br = new BufferedReader(new FileReader(f));
      ReportData result = new ReportData();

      try
      {
         String line;
         while((line = br.readLine()) != null)
         {
            int bi = line.indexOf("JBOSS_HOME=");
            if (bi == -1)
            {
               throw new Exception("JBOSS_HOME= not found in \"" + line + "\"");
            }
            int ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String jbossHome = line.substring(bi + 11, ei);

            bi = line.indexOf("INSTALLATION_TYPE=");
            if (bi == -1)
            {
               throw new Exception("INSTALLATION_TYPE= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String installationType = line.substring(bi + 18, ei);

            bi = line.indexOf("SAR_NAME=");
            if (bi == -1)
            {
               throw new Exception("SAR_NAME= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String sarName = line.substring(bi + 9, ei);

            bi = line.indexOf("EXAMPLE_NAME=");
            if (bi == -1)
            {
               throw new Exception("EXAMPLE_NAME= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String exampleName = line.substring(bi + 13, ei);

            result.addTestRun(jbossHome, installationType, sarName, exampleName);
         }
      }
      finally
      {
         if (br != null)
         {
            br.close();
         }
      }
      return result;
   }

   private void generateReport(ReportData data) throws Exception
   {
      PrintWriter pw = new PrintWriter(new FileWriter(new File(outputDir, outputFileName)));

      List installations = new ArrayList(data.getInstallations());
      Collections.sort(installations);
      List examples = new ArrayList(data.getExamples());
      Collections.sort(examples);

      try
      {
         pw.println("<html>");
         pw.println("<head><title>JBoss Messaging Smoke Test Results</title></head>");
         pw.println("<body>");

         pw.println("<h2>JBoss Messaging Smoke Test Results</h2>");

         pw.print("Java version: ");
         pw.print(System.getProperty("java.version"));
         pw.println("<br>");
         pw.print("Run on: &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
         pw.print(new Date());
         pw.println("<br>");
         pw.println("<br>");


         pw.println("<table border=\"1\" cellpadding=\"2\" cellspacing=\"2\">");

         // header

         pw.print("<tr>");
         pw.print("<td></td>");
         for(Iterator j = examples.iterator(); j.hasNext(); )
         {
            pw.print("<td align=\"center\"><b>");
            pw.print((String)j.next());
            pw.print("</b></td>");
         }
         pw.println("</tr>");


         for(Iterator i = installations.iterator(); i.hasNext();)
         {
            JBossInstallation jbi = (JBossInstallation)i.next();
            Set thisExamples = data.getExamples(jbi);

            pw.println("<tr>");
            pw.print("<td>");
            pw.print(jbi.toString());
            pw.println("</td>");

            for(Iterator j = examples.iterator(); j.hasNext(); )
            {
               String exampleName = (String)j.next();
               if (thisExamples.contains(exampleName))
               {
                  pw.print("<td bgcolor=\"#00FF00\">");
                  pw.print("&nbsp;&nbsp;&nbsp;OK&nbsp;&nbsp;&nbsp; ");
                  pw.println("</td>");
               }
               else
               {
                  pw.print("<td bgcolor=\"#C0C0C0\">");
                  pw.print(" ");
                  pw.println("</td>");
               }
            }

            pw.println("</tr>");
         }

         pw.println("</table>");
         pw.println("</body>");
         pw.println("</html>");
      }
      finally
      {
         if (pw != null)
         {
            pw.close();
         }
      }
   }

   // Inner classes -------------------------------------------------

   private class ReportData
   {
      // <jbossInstallation - Set<examples>>
      private Map tests;

      private ReportData()
      {
         tests = new HashMap();
      }

      public void addTestRun(String jbossHome, String installationType,
                             String sarName, String exampleName) throws Exception
      {

         String jbossVersion;
         boolean installerGenerated = false;
         boolean standalone = false;
         boolean scoped = false;

         int idx = jbossHome.lastIndexOf("jboss-");
         if (idx == -1)
         {
            throw new Exception("Cannot determine JBoss version from " + jbossHome);
         }
         jbossVersion = jbossHome.substring(idx + 6);

         // determine if it's an "installer" generated installation

         File parent = new File(jbossHome).getParentFile();
         while(parent != null)
         {
            if (parent.equals(installerDir))
            {
               installerGenerated = true;
               break;
            }

            parent = parent.getParentFile();
         }

         // determine if is a "standalone" installation

         if ("standalone".equals(installationType))
         {
            standalone = true;
         }

         // determine if it's scoped or not
         scoped = sarName.indexOf("-scoped") != -1;

         JBossInstallation jbi =
            new JBossInstallation(jbossVersion, installerGenerated, standalone, scoped);

         Set examples = (Set)tests.get(jbi);

         if (examples == null)
         {
            examples = new HashSet();
            tests.put(jbi, examples);
         }

         if (examples.contains(exampleName))
         {
            throw new Exception("Duplicate run: " + jbi + ", " + exampleName);
         }
         examples.add(exampleName);
      }


      public Set getInstallations()
      {
         return tests.keySet();
      }

      public Set getExamples(JBossInstallation jbi)
      {
         return (Set)tests.get(jbi);
      }

      /**
       * @return all examples for which at least a test was recorded
       */
      public Set getExamples()
      {
         Set examples = new HashSet();
         for(Iterator i = tests.values().iterator(); i.hasNext();)
         {
            Set s = (Set)i.next();
            examples.addAll(s);
         }
         return examples;
      }
   }

   private class JBossInstallation implements Comparable
   {

      private String version;
      private boolean installerGenerated;
      private boolean standalone;
      private boolean scoped;

      private JBossInstallation(String version,
                                boolean installerGenerated,
                                boolean standalone,
                                boolean scoped)
      {
         this.version = version;
         this.installerGenerated = installerGenerated;
         this.standalone = standalone;
         this.scoped = scoped;
      }

      public int compareTo(Object o)
      {
         JBossInstallation that = (JBossInstallation)o;

         int result = this.version.compareTo(that.version);

         if (result != 0)
         {
            return result;
         }

         int thisScore =
            (this.isStandalone() ? 100 : 0) +
            (this.isInstallerGenerated() ? 10 : 0) +
            (this.isScoped() ? 1 : 0);

         int thatScore =
            (that.isStandalone() ? 100 : 0) +
            (that.isInstallerGenerated() ? 10 : 0) +
            (that.isScoped() ? 1 : 0);

         return thisScore - thatScore;
      }

      public String getVersion()
      {
         return version;
      }

      public boolean isInstallerGenerated()
      {
         return installerGenerated;
      }

      public boolean isStandalone()
      {
         return standalone;
      }

      public boolean isScoped()
      {
         return scoped;
      }

      public boolean equals(Object o)
      {
         if (this == o)
         {
            return true;
         }

         if (!(o instanceof JBossInstallation))
         {
            return false;
         }

         JBossInstallation that = (JBossInstallation)o;

         return
            this.version.equals(that.version) &&
            this.installerGenerated == that.installerGenerated &&
            this.standalone == that.standalone &&
            this.scoped == that.scoped;
      }

      public int hashCode()
      {
         return
            version.hashCode() +
            (installerGenerated ? 17 : 0) +
            (standalone ? 37 : 0) +
            (scoped ? 57 : 0);
      }

      public String toString()
      {
         StringBuffer sb = new StringBuffer();
         sb.append(version);
         sb.append(" (");

         if (standalone)
         {
            sb.append("standalone");
         }
         else
         {
            if (scoped)
            {
               sb.append("scoped");
            }
            else
            {
               sb.append("non-scoped");
            }
         }

         if (installerGenerated)
         {
            sb.append(", installer generated");
         }
         sb.append(")");

         return sb.toString();
      }
   }
}
