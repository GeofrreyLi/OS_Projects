struct lock_t {
    volatile unsigned short next_ticket;
    volatile unsigned short now_serving;
};