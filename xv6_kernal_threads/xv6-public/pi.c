/* test pi estimation; produce-consume pattern to intentionally introduce contention */

#include "types.h"
#include "user.h"
#include "x86.h"

#undef NULL
#define NULL ((void*)0)

#define PGSIZE (4096)
#define BUFFSIZE 5
#define NUM_THREADS 8
#define RAND_MAX 0x7fffffff
uint rseed = 0;

// https://rosettacode.org/wiki/Linear_congruential_generator
uint rand() {
    return rseed = (rseed * 1103515245 + 12345) & RAND_MAX;
}

int TRAILS = 200;

int trails_buf[BUFFSIZE];
int success;
int trail_i;

int ppid;

uint running_flag;
lock_t lock;
#define assert(x) if (x) {} else { \
   printf(1, "%s: %d ", __FILE__, __LINE__); \
   printf(1, "assert failed (%s)\n", # x); \
   printf(1, "TEST FAILED\n"); \
   kill(ppid); \
   exit(); \
}
int count = 0;
void produce(void *arg1, void *arg2);
void consume(void *arg1, void *arg2);

int
main(int argc, char *argv[])
{
  int i;
  ppid = getpid();

  lock_init(&lock);
  trail_i = 0;
  success = 0;
  running_flag = 0;

  for(i = 0; i < NUM_THREADS; i++) {
    assert((thread_create(produce, &TRAILS, NULL)) > 0);
    assert((thread_create(consume, NULL, NULL)) > 0);

  }
  
  //always lock when modifying shared variables
  lock_acquire(&lock);
  running_flag = 1;
  lock_release(&lock);

  for(i = 0; i < NUM_THREADS * 2; i++) {
    int join_pid = thread_join();
    assert(join_pid > 0);
  }

  printf(1, "%d trails inside the circle out of %d trails\n", success, TRAILS * NUM_THREADS);
  int low = 3.13 * (TRAILS * NUM_THREADS) / 4;
  int high = 3.15 * TRAILS * NUM_THREADS / 4;
  //printf(1, "success %d high %d low %d\n", success, high, low);
  
  //should be exact with no randomization
  if(success < high && success > low)
    printf(1, "TEST PASSED\n");
  exit();
}

/*
 * to estimate pi, suppose we have a circle centered at (0, 0) with r = 1
 * let it be externally tangenet to a square centered at  (0, 0) with a = 2
 *
 * A sandom points inside the square has 3.14/4 chance for the
 * point to be inside the circle. Given enough samples,
 * the ratio should be fairly close.
 */
void
produce(void *arg1, void *arg2) {
  int trails = *(int*)arg1;
  while(1) {
           lock_acquire(&lock);
          if (running_flag) {
                  lock_release(&lock);
                        break;
          }
          lock_release(&lock);
  }
  while(trails>0 && running_flag)
  {
    if (trails == 0) {
            break;
    }
    else {
      trails--;
      int x = rand() % 100 * 2 - 100;
      int y = rand() % 100 * 2 - 100;
      int res = (x * x + y * y <= 10000) ? 1 : 0;
      //wait until the buffer has space
      while(1) {
        lock_acquire(&lock);
        if (trail_i < BUFFSIZE) {
          trails_buf[trail_i] = res;
          trail_i++;
          lock_release(&lock);
          break;
        }
        lock_release(&lock);
      }
    }

  }
  lock_acquire(&lock);
  count++;
  if (count == NUM_THREADS) {running_flag = 0;}
  lock_release(&lock);

  exit();
}

void
consume(void *arg1, void *arg2) {
  while(1) {
           lock_acquire(&lock);
          if (running_flag) {
                  lock_release(&lock);
                        break;
          }
          lock_release(&lock);
  }

  while(trail_i>0 || running_flag) {
    int res = 0;
    lock_acquire(&lock);
    if (trail_i > 0) {
            trail_i--;
          res = trails_buf[trail_i];
    }
    success+=res;
    lock_release(&lock);

  }
  exit();
}

