#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

void print_special_string(char *buffer, char *special_string, FILE *fp)
{
	while(fgets(buffer, 80, fp))
	{
		if (strncasecmp(special_string, buffer, strlen(special_string)) == 0) 
		{
      			printf("%s", buffer);
  		}
	}
}

int main(int argc, char *argv[])
{
	char buffer[300]; // store string we have special characters
	int option; // choose command line arguments
	while((option = getopt(argc, argv, "Vhf:"))!=-1)
	{
		switch(option)
		{
			case 'V':
				printf("my-look from CS537 Spring 2022\n");
				exit(0);
				break;
			case 'h':
				printf("This my-look function, and there are 3 options: [V][h][f]\n");
				printf("[-V]: show CS537 information\n");
				printf("[-h]: show help information\n");
				printf("[-f][FILE][String]: open the file and displays all words that match this condition in beginning the string\n");
				exit(0);
				break;
			case 'f':
				if (argc
				FILE *fp = fopen(argv[2],"r");
				if(fp==NULL)
				{
					printf("cannot open file\n");
					exit(1);
				}
				print_special_string(buffer, argv[3], fp);
				fclose(fp);
				exit(0);
				break;
			default:
				printf("my-look: invalid command line\n");
				exit(1);
				break;

		}

		if(argc==2)
		{
			print_special_string(buffer, argv[1], stdin);
			exit(0);
		}
		else
		{
			printf("my-look: invalid command line\n");
    		exit(1);
		}
		return 0;
	}
}
