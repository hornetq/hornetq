package org.hornetq.utils;

/**
 * @author Clebert Suconic
 */

public interface ReferenceCounter
{
   int increment();

   int decrement();
}
