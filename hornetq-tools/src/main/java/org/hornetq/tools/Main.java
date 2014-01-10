package org.hornetq.tools;

public class Main
{
   private final static String USAGE = "Use: java -jar " + getJarName();
   private final static String IMPORT = "import";
   private final static String EXPORT = "export";
   private final static String PRINT_DATA = "print-data";
   private final static String PRINT_PAGES = "print-pages";
   private final static String OPTIONS =  " [" + IMPORT + "|" + EXPORT + "|" + PRINT_DATA + "|" + PRINT_PAGES + "]";

   public static void main(String arg[]) throws Exception
   {
      if (arg.length == 0)
      {
         System.out.println(USAGE + OPTIONS);
         System.exit(-1);
      }

      if (EXPORT.equals(arg[0]))
      {
         if (arg.length != 5)
         {
            System.out.println(USAGE + " " + EXPORT + " <bindings-directory> <message-directory> <page-directory> <large-message-directory>");
            System.exit(-1);
         }
         else
         {
            XmlDataExporter xmlDataExporter = new XmlDataExporter(System.out, arg[1], arg[2], arg[3], arg[4]);
            xmlDataExporter.writeXMLData();
         }
      }
      else if (IMPORT.equals(arg[0]))
      {
         if (arg.length != 6)
         {
            System.out.println(USAGE + " " + IMPORT + " <input-file> <host> <port> <transactional> <application-server-compatibility>");
            System.exit(-1);
         }
         else
         {
            XmlDataImporter xmlDataImporter = new XmlDataImporter(arg[1], arg[2], arg[3], Boolean.parseBoolean(arg[4]), Boolean.parseBoolean(arg[5]));
            xmlDataImporter.processXml();
         }
      }
      else if (PRINT_DATA.equals(arg[0]))
      {
         if (arg.length != 3)
         {
            System.err.println(USAGE + " " + PRINT_DATA + " <bindings-directory> <message-directory>");
            System.exit(-1);
         }

         PrintData.printData(arg[1], arg[2]);
      }
      else if (PRINT_PAGES.equals(arg[0]))
      {
         if (arg.length != 3)
         {
            System.err.println(USAGE + " " + PRINT_PAGES + " <page-directory> <message-directory>");
            System.exit(-1);
         }

         PrintPages.printPages(arg[1], arg[2]);
      }
      else
      {
         System.out.println(USAGE + OPTIONS);
      }
   }

   protected static String getJarName()
   {
      Class klass = Main.class;
      String url = klass.getResource('/' + klass.getName().replace('.', '/') + ".class").toString();
      String jarName = url.substring(0, url.lastIndexOf('!'));
      return jarName.substring(jarName.lastIndexOf('/') + 1);
   }
}