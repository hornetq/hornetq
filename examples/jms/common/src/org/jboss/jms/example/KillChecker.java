/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.jms.example;

import java.io.File;

/**
 * A KillChecker
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class KillChecker extends Thread
{
   private final File file;
   
   public KillChecker(final String dir)
   {      
      file = new File(dir + "/KILL_ME");
   }
   
   @Override
   public void run()
   {      
      while (true)
      {    
         if (file.exists())
         {
            //Hard kill the VM without running any shutdown hooks
            
            //Goodbye!
            
            Runtime.getRuntime().halt(666);
         }
         
         try
         {
            Thread.sleep(50);
         }
         catch (Exception ignore)
         {         
         }
      }
   }  
}
