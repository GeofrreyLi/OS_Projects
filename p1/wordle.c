#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


void print_exclude_string(char *buffer, char *special_string, FILE *fp)
{
	while(fgets(buffer, 300, fp))
	{
		if(strlen(buffer)!=5)
		{
			continue;
		}
		char *p_special_string = special_string;
		int exist=0;
		for(int i=0; i<strlen(special_string); i++)
		{
			char *exclude_special_string = strstr(buffer, p_special_string);
			if(!exclude_special_string)
			{
				exist = 1;
				break;
			}
			p_special_string++;
		}
		if(!exist)
		{
			printf("%s",buffer);
		}
	}
}

int main(int argc, char *argv[])
{
	if(argc!=3)
	{
		printf("wordle: invalid number of args\n");
		exit(1);
	}
	char buffer[300];
	FILE *fp = fopen(argv[1],"r");
        if(fp==NULL)
        {
                printf("wordle: cannot open file\n");
                exit(1);
        } 
	print_exclude_string(buffer, argv[2], fp);

}
