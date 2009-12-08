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

package org.hornetq.jms.tests.tools.ant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Generates a HTML smoke test report based on raw smoke run data.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * $Id$
 */
public class GenerateSmokeReport
{
   // Constants ------------------------------------------------------------------------------------

   public static final String DEFAULT_OUTPUT_BASENAME = "smoke-tes-report";

   private static final byte INSTALLATION_TEST = 0;

   private static final byte CLIENT_COMPATIBILITY_TEST = 1;

   private static final byte SERVER_COMPATIBILITY_TEST = 2;

   // Static ---------------------------------------------------------------------------------------

   public static void main(final String[] args) throws Exception
   {
      new GenerateSmokeReport(args).run();
   }

   /**
    * The method generates a new example list in which examples are ordered according to the
    * ordered name list.
    *
    * @param exampleNames - a List<String>.
    * @param orderedNameList - comma (and space) separated ordered example name list
    *
    * @return a copy of the original list.
    */
   public static List order(final List exampleNames, final String orderedNameList)
   {
      List originalList = new ArrayList(exampleNames);
      List orderedList = new ArrayList();
      for (StringTokenizer st = new StringTokenizer(orderedNameList, ", "); st.hasMoreTokens();)
      {
         String ordn = st.nextToken();
         if (originalList.contains(ordn))
         {
            originalList.remove(ordn);
            orderedList.add(ordn);
         }
      }

      orderedList.addAll(originalList);
      return orderedList;
   }

   // Attributes -----------------------------------------------------------------------------------

   private File inputFile;

   private File outputDir;

   private final String outputFileName;

   private File installerDir;

   private String orderedNameList;

   // Constructors ---------------------------------------------------------------------------------

   private GenerateSmokeReport(final String[] args) throws Exception
   {
      String baseName = null;

      for (int i = 0; i < args.length; i++)
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
         else if ("-order".equals(args[i]))
         {
            if (i == args.length - 1)
            {
               throw new Exception("Example name list must follow -order");
            }
            orderedNameList = args[++i];
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
         baseName = GenerateSmokeReport.DEFAULT_OUTPUT_BASENAME;
      }

      outputFileName = baseName + ".java-" + System.getProperty("java.version") + ".html";
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void run() throws Exception
   {
      ReportData d = parseInputFile(inputFile);
      generateReport(d);
   }

   private ReportData parseInputFile(final File f) throws Exception
   {
      BufferedReader br = new BufferedReader(new FileReader(f));
      ReportData result = new ReportData();

      try
      {
         String line;
         while ((line = br.readLine()) != null)
         {
            int bi = line.indexOf("TEST_TYPE=");
            if (bi == -1)
            {
               throw new Exception("TEST_TYPE= not found in \"" + line + "\"");
            }
            int ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String testType = line.substring(bi + 10, ei);

            bi = line.indexOf("JBOSS_HOME=");
            if (bi == -1)
            {
               throw new Exception("JBOSS_HOME= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String jbossHome = line.substring(bi + 11, ei);

            bi = line.indexOf("JBOSS_CONFIGURATION=");
            if (bi == -1)
            {
               throw new Exception("JBOSS_CONFIGURATION= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String jbossConfiguration = line.substring(bi + 20, ei);

            bi = line.indexOf("CLIENT_VERSION=");
            if (bi == -1)
            {
               throw new Exception("CLIENT_VERSION= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String clientVersion = line.substring(bi + 15, ei);

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

            bi = line.indexOf("SERVER_ARTIFACT_NAME=");
            if (bi == -1)
            {
               throw new Exception("SERVER_ARTIFACT_NAME= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String serverArtifactName = line.substring(bi + 21, ei);

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

            bi = line.indexOf("CLUSTERED=");
            if (bi == -1)
            {
               throw new Exception("CLUSTERED= not found in \"" + line + "\"");
            }
            ei = line.indexOf(' ', bi);
            if (ei == -1)
            {
               ei = line.length();
            }
            String clusteredValue = line.substring(bi + 10, ei);
            clusteredValue = clusteredValue.toLowerCase();
            boolean clustered;
            if ("true".equals(clusteredValue))
            {
               clustered = true;
            }
            else if ("false".equals(clusteredValue))
            {
               clustered = false;
            }
            else
            {
               throw new Exception("CLUSTERED must be either 'true' or 'false' but it's " + clusteredValue);
            }

            result.addTestRun(testType,
                              jbossHome,
                              jbossConfiguration,
                              clientVersion,
                              installationType,
                              serverArtifactName,
                              exampleName,
                              clustered);
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

   private void generateReport(final ReportData data) throws Exception
   {
      PrintWriter pw = new PrintWriter(new FileWriter(new File(outputDir, outputFileName)));

      try
      {
         pw.println("<html>");
         pw.println("<head><title>HornetQ Smoke Test Results</title></head>");
         pw.println("<body>");

         pw.println("<h1>HornetQ Smoke Test Results</h1>");

         pw.print("Java version: ");
         pw.print(System.getProperty("java.version"));
         pw.println("<br>");
         pw.print("Run on: &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
         pw.print(new Date());

         List installations = new ArrayList(data.getInstallations());
         Collections.sort(installations);
         List examples = new ArrayList(data.getExamples(GenerateSmokeReport.INSTALLATION_TEST));
         if (orderedNameList != null)
         {
            examples = GenerateSmokeReport.order(examples, orderedNameList);
         }
         else
         {
            Collections.sort(examples);
         }

         pw.println("<h2>Installation Test Results</h2>");

         pw.println("<table border=\"1\" cellpadding=\"2\" cellspacing=\"2\">");

         // header

         pw.print("<tr>");
         pw.print("<td></td>");
         for (Iterator j = examples.iterator(); j.hasNext();)
         {
            pw.print("<td align=\"center\"><b>");
            pw.print((String)j.next());
            pw.print("</b></td>");
         }
         pw.println("</tr>");

         for (Iterator i = installations.iterator(); i.hasNext();)
         {
            JBossInstallation jbi = (JBossInstallation)i.next();
            Set thisExamples = data.getExamples(jbi);

            pw.println("<tr>");
            pw.print("<td>");
            pw.print(jbi.toString());
            pw.println("</td>");

            for (Iterator j = examples.iterator(); j.hasNext();)
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

         List serverVersions = new ArrayList(data.getServerVersions());
         Collections.sort(serverVersions);
         examples = new ArrayList(data.getExamples(GenerateSmokeReport.CLIENT_COMPATIBILITY_TEST));
         Collections.sort(examples);

         pw.println("<h2>Client Compatibility Test Results</h2>");

         pw.println("<table border=\"1\" cellpadding=\"2\" cellspacing=\"2\">");

         // header

         pw.print("<tr>");
         pw.print("<td></td>");
         for (Iterator j = examples.iterator(); j.hasNext();)
         {
            pw.print("<td align=\"center\"><b>");
            pw.print((String)j.next());
            pw.print("</b></td>");
         }
         pw.println("</tr>");

         for (Iterator i = serverVersions.iterator(); i.hasNext();)
         {
            String serverVersion = (String)i.next();
            Set thisExamples = data.getExamples(true, serverVersion);

            pw.println("<tr>");
            pw.print("<td>");
            pw.print(serverVersion);
            pw.println("</td>");

            for (Iterator j = examples.iterator(); j.hasNext();)
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

         List clientVersions = new ArrayList(data.getClientVersions());
         Collections.sort(clientVersions);
         examples = new ArrayList(data.getExamples(GenerateSmokeReport.SERVER_COMPATIBILITY_TEST));
         Collections.sort(examples);

         pw.println("<h2>Server Compatibility Test Results</h2>");

         pw.println("<table border=\"1\" cellpadding=\"2\" cellspacing=\"2\">");

         // header

         pw.print("<tr>");
         pw.print("<td></td>");
         for (Iterator j = examples.iterator(); j.hasNext();)
         {
            pw.print("<td align=\"center\"><b>");
            pw.print((String)j.next());
            pw.print("</b></td>");
         }
         pw.println("</tr>");

         for (Iterator i = clientVersions.iterator(); i.hasNext();)
         {
            String clientVersion = (String)i.next();
            Set thisExamples = data.getExamples(false, clientVersion);

            pw.println("<tr>");
            pw.print("<td>");
            pw.print(clientVersion);
            pw.println("</td>");

            for (Iterator j = examples.iterator(); j.hasNext();)
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
      private final Map installationTests;

      // <serverVersion - Set<example>>
      private final Map clientCompatibilityTests;

      // <clientVersion - Set<example>>
      private final Map serverCompatibilityTests;

      private ReportData()
      {
         installationTests = new HashMap();
         clientCompatibilityTests = new HashMap();
         serverCompatibilityTests = new HashMap();
      }

      public void addTestRun(final String testType,
                             final String jbossHome,
                             final String jbossConfiguration,
                             final String clientVersion,
                             final String installationType,
                             final String serverArtifactName,
                             final String exampleName,
                             final boolean clustered) throws Exception
      {
         if ("installation".equals(testType))
         {
            addInstallationTestRun(jbossHome, installationType, serverArtifactName, exampleName, clustered);
         }
         else if ("client.compatibility".equals(testType))
         {
            addClientCompatibilityTestRun(jbossConfiguration, exampleName);
         }
         else if ("server.compatibility".equals(testType))
         {
            addServerCompatibilityTestRun(clientVersion, exampleName);
         }
         else
         {
            throw new Exception("Unknown test type: " + testType);
         }
      }

      public Set getInstallations()
      {
         return installationTests.keySet();
      }

      public Set getServerVersions()
      {
         return clientCompatibilityTests.keySet();
      }

      public Set getClientVersions()
      {
         return serverCompatibilityTests.keySet();
      }

      public Set getExamples(final JBossInstallation jbi)
      {
         return (Set)installationTests.get(jbi);
      }

      public Set getExamples(final boolean clientTest, final String version)
      {
         if (clientTest)
         {
            return (Set)clientCompatibilityTests.get(version);
         }
         return (Set)serverCompatibilityTests.get(version);
      }

      /**
       * @return all examples for which at least a test was recorded
       */
      public Set getExamples(final byte testType)
      {
         Set examples = new HashSet();
         Collection values = testType == GenerateSmokeReport.INSTALLATION_TEST ? installationTests.values()
                                                                              : testType == GenerateSmokeReport.CLIENT_COMPATIBILITY_TEST ? clientCompatibilityTests.values()
                                                                                                                                         : serverCompatibilityTests.values();
         for (Iterator i = values.iterator(); i.hasNext();)
         {
            Set s = (Set)i.next();
            examples.addAll(s);
         }
         return examples;
      }

      private void addInstallationTestRun(final String jbossHome,
                                          final String installationType,
                                          final String serverArtifactName,
                                          final String exampleName,
                                          final boolean clustered) throws Exception
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
         while (parent != null)
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
         scoped = serverArtifactName.indexOf("-scoped") != -1;

         JBossInstallation jbi = new JBossInstallation(jbossVersion, installerGenerated, standalone, scoped, clustered);

         Set examples = (Set)installationTests.get(jbi);

         if (examples == null)
         {
            examples = new HashSet();
            installationTests.put(jbi, examples);
         }

         if (examples.contains(exampleName))
         {
            throw new Exception("Duplicate installation run: " + jbi + ", " + exampleName);
         }
         examples.add(exampleName);
      }

      private void addClientCompatibilityTestRun(final String jbossConfiguration, final String exampleName) throws Exception
      {
         if (!jbossConfiguration.startsWith("messaging-"))
         {
            throw new Exception("Invalid JBoss configuration name for a " + "client compatibility test: " +
                                jbossConfiguration);
         }

         String serverVersion = jbossConfiguration.substring(10);

         Set examples = (Set)clientCompatibilityTests.get(serverVersion);
         if (examples == null)
         {
            examples = new HashSet();
            clientCompatibilityTests.put(serverVersion, examples);
         }

         if (examples.contains(exampleName))
         {
            throw new Exception("Duplicate client compatibility run: " + exampleName +
                                " on " +
                                serverVersion +
                                " server");
         }
         examples.add(exampleName);
      }

      private void addServerCompatibilityTestRun(final String clientVersion, final String exampleName) throws Exception
      {
         Set examples = (Set)serverCompatibilityTests.get(clientVersion);
         if (examples == null)
         {
            examples = new HashSet();
            serverCompatibilityTests.put(clientVersion, examples);
         }

         if (examples.contains(exampleName))
         {
            throw new Exception("Duplicate server compatibility run: " + exampleName +
                                " with " +
                                clientVersion +
                                " client");
         }
         examples.add(exampleName);
      }
   }

   private class JBossInstallation implements Comparable
   {

      private final String version;

      private final boolean installerGenerated;

      private final boolean standalone;

      private final boolean scoped;

      private final boolean clustered;

      private JBossInstallation(final String version,
                                final boolean installerGenerated,
                                final boolean standalone,
                                final boolean scoped,
                                final boolean clustered)
      {
         this.version = version;
         this.installerGenerated = installerGenerated;
         this.standalone = standalone;
         this.scoped = scoped;
         this.clustered = clustered;
      }

      public int compareTo(final Object o)
      {
         JBossInstallation that = (JBossInstallation)o;

         int result = version.compareTo(that.version);

         if (result != 0)
         {
            return result;
         }

         int thisScore = (isClustered() ? 1000 : 0) + (isStandalone() ? 100 : 0) +
                         (isInstallerGenerated() ? 10 : 0) +
                         (isScoped() ? 1 : 0);

         int thatScore = (that.isClustered() ? 1000 : 0) + (that.isStandalone() ? 100 : 0) +
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

      public boolean isClustered()
      {
         return clustered;
      }

      public boolean equals(final Object o)
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

         return version.equals(that.version) && installerGenerated == that.installerGenerated &&
                standalone == that.standalone &&
                scoped == that.scoped &&
                clustered == that.clustered;
      }

      public int hashCode()
      {
         return version.hashCode() + (installerGenerated ? 17 : 0) +
                (standalone ? 37 : 0) +
                (scoped ? 57 : 0) +
                (clustered ? 129 : 0);
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

         if (clustered)
         {
            sb.append(", CLUSTERED");
         }
         else
         {
            sb.append(", not manageConfirmations");
         }

         sb.append(")");

         return sb.toString();
      }

   }
}
