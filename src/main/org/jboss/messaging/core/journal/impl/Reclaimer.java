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

package org.jboss.messaging.core.journal.impl;

import org.jboss.messaging.core.logging.Logger;


/**
 * 
 * <p>A ReclaimerTest</p>
 * 
 * <p>The journal consists of an ordered list of journal files Fn where 0 <= n <= N</p>
 * 
 * <p>A journal file can contain either positives (pos) or negatives (neg)</p>
 * 
 * <p>(Positives correspond either to adds or updates, and negatives correspond to deletes).</p>
 * 
 * <p>A file Fn can be deleted if, and only if the following criteria are satisified</p>
 * 
 * <p>1) All pos in a file Fn, must have corresponding neg in any file Fm where m >= n.</p>
 * 
 * <p>2) All pos that correspond to any neg in file Fn, must all live in any file Fm where 0 <= m <= n
 * which are also marked for deletion in the same pass of the algorithm.</p>
 * 
 * <p>WIKI Page: <a href="http://wiki.jboss.org/wiki/JBossMessaging2Reclaiming">http://wiki.jboss.org/wiki/JBossMessaging2Reclaiming</a></p>
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class Reclaimer
{
   private static final Logger log = Logger.getLogger(Reclaimer.class);
   
   public void scan(final JournalFile[] files)
   {
      for (int i = 0; i < files.length; i++)
      {
         //First we evaluate criterion 1)
         
         JournalFile currentFile = files[i];
         
         int posCount = currentFile.getPosCount();
         
         int totNeg = 0;
         
         for (int j = i; j < files.length; j++)
         {
            totNeg += files[j].getNegCount(currentFile);
         }
         
         currentFile.setCanReclaim(true);
         
         if (posCount <= totNeg)
         {   		
            //Now we evaluate criterion 2)
            
            for (int j = 0; j <= i; j++)
            {
               JournalFile file = files[j];
               
               int negCount = currentFile.getNegCount(file);
               
               if (negCount != 0)
               {
                  if (file.isCanReclaim())
                  {
                     //Ok
                  }
                  else
                  {
                     currentFile.setCanReclaim(false);
                     
                     break;
                  }
               }
            }   		
         }
         else
         {
            currentFile.setCanReclaim(false);
         }			
      }			
   }
}
