typedef unsigned int   uint;
typedef unsigned short ushort;
typedef unsigned char  uchar;
typedef uint pde_t;
typedef struct _lock_t {
    int next_ticket;
    int now_serving;
}lock_t;
