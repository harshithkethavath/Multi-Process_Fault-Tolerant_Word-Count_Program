#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h> 

#define MAX_PROC 100
#define MAX_FORK 1000


typedef struct count_t {
	int linecount;
	int wordcount;
	int charcount;
} count_t;

typedef struct job_t {
    int id;
    pid_t pid;
    long offset; 
    long size;
    int pipefd[2];
	int status;
} job_t;


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
		if (ch != ' ' && ch != '\n') { ++count.charcount; }
		if (ch == ' ' || ch == '\n') { ++count.wordcount; }
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


void create_child(job_t* job, const char* filename) {

    if (pipe(job->pipefd) == -1) {
        perror("pipe failed");
        exit(EXIT_FAILURE);
    }

    pid_t pid = fork();
    if(pid < 0) {
        perror("fork failed");
        exit(EXIT_FAILURE);
    } else if (pid == 0) {
        
        close(job->pipefd[0]); 

        FILE* fp = fopen(filename, "r");
        if (fp == NULL) {
            perror("fopen failed in child");
            exit(EXIT_FAILURE);
        }

        count_t count = word_count(fp, job->offset, job->size);

        write(job->pipefd[1], &count, sizeof(count_t));

        close(job->pipefd[1]);
        fclose(fp);
        
        exit(0);
    } else {

        close(job->pipefd[1]);

        job->pid = pid;
    }
}


int main(int argc, char **argv)
{
	long fsize;
	int numJobs;
	job_t jobs[MAX_PROC];
	count_t total, count_from_child;
	int i, status;
    pid_t child_pid;

	if(argc < 3) {
		printf("usage: wc_mul <# of processes> <filename> [crash_rate]\n");
		return 0;
	}

	numJobs = atoi(argv[1]);
	if(numJobs > MAX_PROC) numJobs = MAX_PROC;
    if(numJobs <= 0) numJobs = 1;

	if(argc > 3) {
		CRASH = atoi(argv[3]);
		if(CRASH < 0) CRASH = 0;
		if(CRASH > 50) CRASH = 50;
	}
	printf("CRASH RATE: %d%%\n", CRASH);

	total.linecount = 0;
	total.wordcount = 0;
	total.charcount = 0;

	FILE* fp = fopen(argv[2], "r");
	if(fp == NULL) {
		printf("File open error: %s\n", argv[2]);
		return 0;
	}
	fseek(fp, 0L, SEEK_END);
	fsize = ftell(fp);
	fclose(fp);

    long chunk_size = fsize / numJobs;
	for(i = 0; i < numJobs; i++) {
        jobs[i].id = i;
        jobs[i].offset = i * chunk_size;
        jobs[i].status = 0;

        if (i == numJobs - 1) {
            jobs[i].size = fsize - jobs[i].offset;
        } else {
            jobs[i].size = chunk_size;
        }
        
        create_child(&jobs[i], argv[2]);
	}

    int completed_jobs = 0;
    while (completed_jobs < numJobs) {

        child_pid = waitpid(-1, &status, 0);
        if (child_pid < 0) {
            perror("waitpid error");
            continue;
        }

        int job_index = -1;
        for (i = 0; i < numJobs; i++) {
            if (jobs[i].pid == child_pid) {
                job_index = i;
                break;
            }
        }
        if (job_index == -1) continue;


        if (WIFEXITED(status)) {

            printf("[pid %d] finished normally.\n", child_pid);

            read(jobs[job_index].pipefd[0], &count_from_child, sizeof(count_t));
            
            total.linecount += count_from_child.linecount;
            total.wordcount += count_from_child.wordcount;
            total.charcount += count_from_child.charcount;
            
            jobs[job_index].status = 1;
            close(jobs[job_index].pipefd[0]);
            completed_jobs++;

        } else if (WIFSIGNALED(status)) {

            printf("[pid %d] terminated abnormally! Re-creating child for job %d.\n", child_pid, jobs[job_index].id);
            
            close(jobs[job_index].pipefd[0]);
			
            create_child(&jobs[job_index], argv[2]);
        }
    }

	printf("\n========== Final Results ================\n");
	printf("Total Lines : %d \n", total.linecount);
	printf("Total Words : %d \n", total.wordcount);
	printf("Total Characters : %d \n", total.charcount);
	printf("=========================================\n");

	return(0);
}