/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jboss.messaging.core.util.ServerDelegate;

import java.io.Serializable;


/**
 * Wraps togeter the methods to be invoked remotely by the distributed pipe input endpoints.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
interface PipeOutputServerDelegate extends ServerDelegate
{

   /**
    * The metohd to be called remotely by the input end of the distributed pipe.
    *
    * @return the acknowledgement returned by the associated receiver.
    */
   // TODO - review core refactoring 2
//   public Acknowledgment handle(Routable r);


   /**
    * The metohd to be called remotely by the input end of the distributed pipe.
    *
    * @return the receiver ID of the attached receiver or null if there is no receiver.
    */
   public Serializable getOutputID();

}
