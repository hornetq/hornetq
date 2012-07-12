package org.hornetq.rest;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class UnmarshalException extends RuntimeException
{
   private static final long serialVersionUID = 3932027442263719425L;

   public UnmarshalException(String s)
   {
      super(s);
   }

   public UnmarshalException(String s, Throwable throwable)
   {
      super(s, throwable);
   }
}
