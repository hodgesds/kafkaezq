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
    zlist_t *topics;
    int run;
    bool debug;
    int32_t partition;
    int msg_size;
    char *null_str;
    char errstr[512];
    char *group;
};


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
    self->debug = true;
    self->partition = RD_KAFKA_PARTITION_UA;
    self->msg_size = 1024*1024;
    self->null_str = "NULL";
    
    self->rk_conf = rd_kafka_conf_new ();
    self->rkt_conf = rd_kafka_topic_conf_new ();

    self->topics = zlist_new ();

  
    return self;
}


//  --------------------------------------------------------------------------
//  Add a topic to our subscription list

void
ksock_set_subscribe (ksock_t *self, char *topic)
{
    zlist_append (self->topics, topic);
}


//  --------------------------------------------------------------------------
//  Connect the socket to kafka brokers

void
ksock_connect (ksock_t *self, char *brokers)
{
    int rc = rd_kafka_conf_set (self->rk_conf, "group.id", "mygroup", self->errstr, sizeof (self->errstr));
    assert (rc == RD_KAFKA_CONF_OK);

    char tmp[16];
    snprintf(tmp, sizeof(tmp), "%i", SIGIO);
	rd_kafka_conf_set(self->rk_conf, "internal.termination.signal", tmp, NULL, 0);

    rc = rd_kafka_topic_conf_set (self->rkt_conf, "offset.store.method", "broker", self->errstr, sizeof (self->errstr));
    assert (rc == RD_KAFKA_CONF_OK);

    rd_kafka_conf_set_rebalance_cb(self->rk_conf, rebalance_cb);
    rd_kafka_conf_set_default_topic_conf (self->rk_conf, self->rkt_conf);
    
    self->rk = rd_kafka_new (RD_KAFKA_CONSUMER, self->rk_conf, self->errstr, sizeof (self->errstr));
    assert (self->rk);

    
    if (self->debug)
        rd_kafka_set_log_level (self->rk, LOG_DEBUG);

    rc = rd_kafka_brokers_add (self->rk, "localhost:9092");
    assert (rc > 0);

    rd_kafka_poll_set_consumer (self->rk);

    self->topiclist = rd_kafka_topic_partition_list_new (zlist_size (self->topics));
    char *topic = (char *) zlist_first (self->topics);
    while (topic) {
        rd_kafka_topic_partition_list_add (self->topiclist, topic, -1);
        topic = (char *) zlist_next (self->topics);
    }

    self->err = rd_kafka_subscribe (self->rk, self->topiclist);
    if (self->err) {
        fprintf (stderr, "%s\n", rd_kafka_err2str (self->err));
    }
}


//  --------------------------------------------------------------------------
//  Receive a message

rd_kafka_message_t *
ksock_recv (ksock_t *self)
{
    rd_kafka_message_t *msg = rd_kafka_consumer_poll (self->rk, 1000);
    return msg;
}

//  --------------------------------------------------------------------------
//  Destroy the ksock

void
ksock_destroy (ksock_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        ksock_t *self = *self_p;
        rd_kafka_consumer_close (self->rk);
        rd_kafka_topic_partition_list_destroy (self->topiclist);
        rd_kafka_destroy (self->rk);
        zlist_destroy (&self->topics);
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

    ksock_set_subscribe (self, "test");
    ksock_connect (self, "localhost:9092");
    rd_kafka_message_t *msg = ksock_recv (self);
    rd_kafka_message_destroy (msg);
    ksock_destroy (&self);

    //  @end
    printf ("OK\n");
}
