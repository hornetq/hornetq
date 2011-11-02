package org.hornetq.rest;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class UnknownMediaType extends RuntimeException
{
   private static final long serialVersionUID = -1445038845165315001L;

   public UnknownMediaType(String s)
   {
      super(s);
   }
}
