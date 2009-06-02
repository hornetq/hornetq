
#include <stdio.h>
#include <stdlib.h>
#include <libaio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


int main(int arg, char * param[])
{
   char * directory;
   int numberOfFiles;
   int fileSize = 10 * 1024 * 1024;
   int bufferSize = 1024 * 1024;
   void * preAllocBuffer = 0;
   int i = 0;

   if (arg != 3)
   {
       fprintf (stderr, "usage disktest <directory> <numberOfFiles>\n");
       exit(-1);
   }

   directory = param[1];
   numberOfFiles = atoi(param[2]);
   fileSize = atoi(param[3]);
   bufferSize = atoi(param[4]);

   fprintf (stderr, "allocating file");
   if (posix_memalign(&preAllocBuffer, 512, bufferSize))
   {
       fprintf (stderr, "Error allocating buffer");
       exit(-1);
   }

   for (i = 0 ; i < numberOfFiles; i++)
   {
      fprintf (stderr, "I'm here\n");
      char file[1024];
      sprintf (file, "%s/file%d.dat", directory, i);
      fprintf (stderr, "creating file %s\n", file);
      open (file, O_RDWR | O_CREAT | O_DIRECT, 0666);
   }


   

   

   
   return (0);

   
}
