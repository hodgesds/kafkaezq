/*  =========================================================================
    ksock - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.       
    This file is part of kafkaezq.                                     
                                                                       
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.           
    =========================================================================
*/

/*
@header
    ksock - 
@discuss
@end
*/

#include "kafkaezq_classes.h"

//  Structure of our class

struct _ksock_t {
    rd_kafka_t *rk;
    rd_kafka_conf_t *rk_conf;
    rd_kafka_topic_conf_t *rkt_conf;
    rd_kafka_topic_partition_list_t *topiclist;
    rd_kafka_resp_err_t err;
    int run;
    int debug;
    int32_t partition;
    int msg_size;
    char *null_str;
    char errstr[512];
};


//  --------------------------------------------------------------------------
//  Default error callback

static void 
s_err_callback (rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    const char *errstr = rd_kafka_err2str (rd_kafka_errno2err(err));
	printf("%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), errstr, reason);
}


//  --------------------------------------------------------------------------
//  Default rebalance callback

static void
rebalance_cb (rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque)
{
	switch (err) {
	    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
		    rd_kafka_assign(rk, partitions);
		    break;

	    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            rd_kafka_assign(rk, NULL);
            break;

        default:
            break;
    }
}


//  --------------------------------------------------------------------------
//  Create a new ksock

ksock_t *
ksock_new ()
{
    ksock_t *self = (ksock_t *) zmalloc (sizeof (ksock_t));
    assert (self);

    self->run  = 1;
    self->debug = 1;
    self->partition = RD_KAFKA_PARTITION_UA;
    self->msg_size = 1024*1024;
    self->null_str = "NULL";
    
    self->rk_conf = rd_kafka_conf_new ();
    self->rkt_conf = rd_kafka_topic_conf_new ();

    rd_kafka_conf_set_rebalance_cb(self->rk_conf, rebalance_cb);
    rd_kafka_conf_set_default_topic_conf (self->rk_conf, self->rkt_conf);
   
    return self;
}

void
ksock_connect (ksock_t *self, char *brokers)
{
    int rc = rd_kafka_conf_set (self->rk_conf, "metadata.broker.list", brokers, self->errstr, sizeof (self->errstr));
    assert (rc == RD_KAFKA_CONF_OK);

    rd_kafka_conf_set_error_cb (self->rk_conf, s_err_callback);
    assert (rc == RD_KAFKA_CONF_OK);

    struct timeval tv;
	gettimeofday(&tv, NULL);
	srand(tv.tv_usec);

    char tmp[16];
    snprintf (tmp, sizeof (tmp), "%i", SIGIO);

    rd_kafka_conf_set(self->rk_conf, "internal.termination.signal", tmp, NULL, 0);
    
    rd_kafka_conf_set_log_cb(self->rk_conf, rd_kafka_log_print);

    self->rk = rd_kafka_new (RD_KAFKA_CONSUMER, self->rk_conf, self->errstr, sizeof (self->errstr));
    assert (self->rk);
    
    self->rk_conf = NULL; 
    self->rkt_conf = NULL;

    rd_kafka_poll_set_consumer (self->rk);
    rd_kafka_set_log_level (self->rk, LOG_DEBUG);
}


//  --------------------------------------------------------------------------
//  Destroy the ksock

void
ksock_destroy (ksock_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        ksock_t *self = *self_p;
        rd_kafka_destroy (self->rk);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

//  --------------------------------------------------------------------------
//  Self test of this class

void
ksock_test (bool verbose)
{
    printf (" * ksock: ");

    //  @selftest
    //  Simple create/destroy test
    ksock_t *self = ksock_new ();
    assert (self);

    ksock_connect (self, "localhost");
    ksock_destroy (&self);
    //  @end
    printf ("OK\n");
}
