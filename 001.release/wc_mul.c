/*************************************************
 * C program to count no of lines, words and 	 *
 * characters in a file.		                 *
 *************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <unistd.h>
#include <time.h>
#include <stdint.h>

#define MAX_PROC 100
#define MAX_FORK 1000
#define READ_END 0
#define WRITE_END 1
#define MAX_RETRIES 10

typedef struct count_t {
	int linecount;
	int wordcount;
	int charcount;
} count_t;

typedef struct plist_t {
	int pid;
	int offset;
	int pipefd[2];
	long current_chunk_size;
	int retries;
	int complete;
} plist_t;

int CRASH = 0;

count_t word_count(FILE* fp, long offset, long size)
{
	char ch;
	long rbytes = 0;

	count_t count;
	count.linecount = 0;
	count.wordcount = 0;
	count.charcount = 0;

	printf("[pid %d] reading %ld bytes from offset %ld\n", getpid(), size, offset);

	if(fseek(fp, offset, SEEK_SET) < 0) {
		printf("[pid %d] fseek error!\n", getpid());
	}

	while ((ch=getc(fp)) != EOF && rbytes < size) {

		// Increment character count if NOT new line or space
		if (ch != ' ' && ch != '\n') ++count.charcount;
		// Increment word count if new line or space character
		if (ch == ' ' || ch == '\n') ++count.wordcount;
		// Increment line count if new line character
		if (ch == '\n') ++count.linecount;
		
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
	plist_t plist[MAX_PROC];
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

	// number of child processes to fork
	numJobs = atoi(argv[1]);
	if(numJobs > MAX_PROC) numJobs = MAX_PROC;

	total.linecount = total.wordcount = total.charcount = 0;

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

	long base_chunk_size = fsize / numJobs;
	long remainder = fsize % numJobs;
	long current_offset = 0;

	for(i = 0; i < numJobs; i++) {
		
		plist[i].current_chunk_size = base_chunk_size + (i < remainder ? 1 : 0);
		plist[i].offset = current_offset;
		plist[i].retries = 0;
		plist[i].complete = 0;

		if (pipe(plist[i].pipefd) == -1) {
			fprintf(stderr, "Fork Failed");
			return 1;
		}

		if(nFork++ > MAX_FORK) return 0;

		if((plist[i].pid = fork()) < 0) {
			printf("Fork failed.\n");
		} else if(plist[i].pid == 0) {
			
			fp = fopen(argv[2], "r");
			count_t count = word_count(fp, plist[i].offset, plist[i].current_chunk_size);
			close(plist[i].pipefd[READ_END]); 
			write(plist[i].pipefd[WRITE_END], &count, sizeof(count));
			fclose(fp);
			close(plist[i].pipefd[WRITE_END]); 
			return 0;

		} else if (plist[i].pid > 0) {

			close(plist[i].pipefd[WRITE_END]);
			current_offset += plist[i].current_chunk_size;

		}
	}

	while (nFork > 0) {
		pid = waitpid(-1, &status, 0);
		for (i = 0; i < numJobs; i++) {
				
			if (plist[i].pid == pid && !plist[i].complete) {

				count_t temp;
				ssize_t bytesRead = 0;
				char *ptr = (char*)&temp;

				while (bytesRead < sizeof(temp)) {
					ssize_t n = read(plist[i].pipefd[READ_END], ptr + bytesRead, sizeof(temp) - bytesRead);
					if (n <= 0) break;
					bytesRead += n;
				}

				close(plist[i].pipefd[READ_END]);
			
				if (WIFEXITED(status)) { 
						
					if (bytesRead == sizeof(temp)) {
						total.linecount += temp.linecount;
						total.wordcount += temp.wordcount;
						total.charcount += temp.charcount;
					}

					plist[i].complete = 1;
					nFork--;

				} else if (WIFSIGNALED(status)) {
						
					if(plist[i].retries++ >= MAX_RETRIES) {
                       	printf("[offset %d] reached max retries\n", plist[i].offset);
                       	plist[i].complete = 1;
                       	nFork--;
                       	break;
                   	}

					if (pipe(plist[i].pipefd) == -1) {
						fprintf(stderr, "Fork Failed");
						return 1;
					}
					if((plist[i].pid = fork()) < 0) {
						printf("Fork failed.\n");
					} else if (plist[i].pid == 0) {

						fp = fopen(argv[2], "r");
						count_t count = word_count(fp, plist[i].offset, plist[i].current_chunk_size);
						close(plist[i].pipefd[READ_END]); 
						write(plist[i].pipefd[WRITE_END], &count, sizeof(count));

						fclose(fp);
						close(plist[i].pipefd[WRITE_END]);

						printf("retry child: %d\n", i);

						return 0;
					} else {
						close(plist[i].pipefd[WRITE_END]);
					}
				}
				break;
			} 
		}
	}

	printf("\n========== Final Results ================\n");
	printf("Total Lines : %d \n", total.linecount);
	printf("Total Words : %d \n", total.wordcount);
	printf("Total Characters : %d \n", total.charcount);
	printf("=========================================\n");

	return(0);
}

