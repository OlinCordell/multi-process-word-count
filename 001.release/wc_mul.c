/*************************************************
 * C program to count no of lines, words and 	 *
 * characters in a file.		                 *
 *************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>

#define MAX_PROC 100
#define MAX_FORK 1000

typedef struct count_t {
	int linecount;
	int wordcount;
	int charcount;
} count_t;

/*
typedef struct plist_t {
	int pid;
	int offset;
	int pipefd[2];
} plist_t;
*/

int CRASH = 0;

count_t word_count(FILE* fp, long offset, long size)
{
	char ch;
	long rbytes = 0;

	count_t count;
	// Initialize counter variables
	count.linecount = 0;
	count.wordcount = 0;
	count.charcount = 0;

	printf("[pid %d] reading %ld bytes from offset %ld\n", getpid(), size, offset);

	if(fseek(fp, offset, SEEK_SET) < 0) {
		printf("[pid %d] fseek error!\n", getpid());
	}

	while ((ch=getc(fp)) != EOF && rbytes < size) {
		// Increment character count if NOT new line or space
		if (ch != ' ' && ch != '\n') { ++count.charcount; }

		// Increment word count if new line or space character
		if (ch == ' ' || ch == '\n') { ++count.wordcount; }

		// Increment line count if new line character
		if (ch == '\n') { ++count.linecount; }
		rbytes++;
	}

	srand(getpid());
	if(CRASH > 0 && (rand()%100 < CRASH))
	{
		printf("[pid %d] crashed.\n", getpid());
		abort();
	}

	return count;
}

int main(int argc, char **argv)
{
	long fsize;
	FILE *fp;
	int numJobs;
	//plist_t plist[MAX_PROC];
	count_t total, count;
	int i, pid, status;
	int nFork = 0;

	if(argc < 3) {
		printf("usage: wc_mul <# of processes> <filname>\n");
		return 0;
	}

	if(argc > 3) {
		CRASH = atoi(argv[3]);
		if(CRASH < 0) CRASH = 0;
		if(CRASH > 50) CRASH = 50;
	}
	printf("CRASH RATE: %d\n", CRASH);


	numJobs = atoi(argv[1]);
	if(numJobs > MAX_PROC) numJobs = MAX_PROC;

	total.linecount = 0;
	total.wordcount = 0;
	total.charcount = 0;

	// Open file in read-only mode
	fp = fopen(argv[2], "r");

	if(fp == NULL) {
		printf("File open error: %s\n", argv[2]);
		printf("usage: wc <# of processes> <filname>\n");
		return 0;
	}

	fseek(fp, 0L, SEEK_END);
	fsize = ftell(fp);

	fclose(fp);
	// calculate file offset and size to read for each child

	for(i = 0; i < numJobs; i++) {
		//set pipe;

		if(nFork++ > MAX_FORK) return 0;

		pid = fork();
		if(pid < 0) {
			printf("Fork failed.\n");
		} else if(pid == 0) {
			// Child
			fp = fopen(argv[2], "r");
			count = word_count(fp, 0, fsize);
			// send the result to the parent through pipe
			fclose(fp);
			return 0;
		}
	}

	// Parent
	// wait for all children
	// check their exit status
	// read the result of normalliy terminated child
	// re-crete new child if there is one or more failed child
	// close pipe

	printf("\n========== Final Results ================\n");
	printf("Total Lines : %d \n", count.linecount);
	printf("Total Words : %d \n", count.wordcount);
	printf("Total Characters : %d \n", count.charcount);
	printf("=========================================\n");

	return(0);
}

