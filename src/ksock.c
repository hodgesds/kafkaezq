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

static void 
dump (const char *name, const void *ptr, size_t len)
{
    const char *p = (const char *)ptr;
    unsigned int of = 0;


    if (name) {
        zproc_log_notice ("%s ump (%zd bytes):\n", name, len);
    }

    for (of = 0 ; of < len ; of += 16) {
        char hexen[16*3+1];
        char charen[16+1];
        int hof = 0;

        int cof = 0;
        int i;

        for (i = of ; i < (int)of + 16 && i < (int)len ; i++) {
            hof += sprintf(hexen+hof, "%02x ", p[i] & 0xff);
            cof += sprintf(charen+cof, "%c", isprint((int)p[i]) ? p[i] : '.');
        }
        zproc_log_notice ("%08x: %-48s %-16s\n", of, hexen, charen);
    }
}


//  Structure of our class

struct _ksock_t {
    rd_kafka_t *rk;
    rd_kafka_conf_t *rk_conf;
    rd_kafka_topic_conf_t *rkt_conf;
    rd_kafka_topic_partition_list_t *topiclist;
    rd_kafka_resp_err_t err;
    zlist_t *topics;
    int run;
    int exit_eof;
    int wait_eof;
    int quiet;
    bool debug;
    int32_t partition;
    char errstr[512];
    const char *group;
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
    zsys_init ();
    ksock_t *self = (ksock_t *) zmalloc (sizeof (ksock_t));
    assert (self);

    self->run  = 1;
    self->exit_eof = 0;
    self->wait_eof = 0;
    self->quiet = 0;
    self->debug = true;
    self->partition = RD_KAFKA_PARTITION_UA;
    
    self->rk_conf = rd_kafka_conf_new ();
    self->rkt_conf = rd_kafka_topic_conf_new ();

    self->topics = zlist_new ();

  
    return self;
}


//  --------------------------------------------------------------------------
//  Set a consumer group

void
ksock_set_group (ksock_t *self, const char *group)
{
    self->group = group;
}




//  --------------------------------------------------------------------------
//  Add a topic to our subscription list

void
ksock_set_subscribe (ksock_t *self, char *topic)
{
    zlist_append (self->topics, topic);
}


void 
zsock_handle_msg (ksock_t *self, rd_kafka_message_t *msg, void *opaque)
{
    if (msg->err) {
        if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            zproc_log_notice ("Consumer reached end of %s [%"PRId32"] "
                    "message queue at offset %"PRId64"\n",
                    rd_kafka_topic_name(msg->rkt),
                    msg->partition, msg->offset);

            if (self->exit_eof && --self->wait_eof == 0) {
                zproc_log_notice ("All parition(s) reached EOF: exiting\n");
                self->run = 0;
            }
            return;
        }

        if (msg->rkt) {
            zproc_log_notice (
                    "Consume error for "
                    "topic \"%s\" [%"PRId32"] "
                    "offset %"PRId64": %s\n",
                    rd_kafka_topic_name(msg->rkt),
                    msg->partition,
                    msg->offset,
                    rd_kafka_message_errstr(msg));
        }

        else {
            zproc_log_notice (
                    "Consumer error: %s: %s\n",
                    rd_kafka_err2str(msg->err),
                    rd_kafka_message_errstr(msg));
        }

        if (msg->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
                    msg->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC) {
            self->run = 0;
            return;
        }
    }

    if (!self->quiet) {
        zproc_log_notice (
                "Message (topic %s [%"PRId32"], "
                "offset %"PRId64", %zd bytes):\n",
                rd_kafka_topic_name(msg->rkt),
                msg->partition,
                msg->offset, msg->len);
    }

    if (msg->key_len) {
            dump("Message Key", msg->key, msg->key_len);
    }

    dump("Message Payload", msg->payload, msg->len);

    return;
}


//  --------------------------------------------------------------------------
//  Connect the socket to kafka brokers

void
ksock_connect (ksock_t *self, char *brokers)
{
    int rc = rd_kafka_conf_set (self->rk_conf, "group.id", self->group, self->errstr, sizeof (self->errstr));
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

    rc = rd_kafka_brokers_add (self->rk, brokers);
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
    rd_kafka_message_t *msg = rd_kafka_consumer_poll (self->rk, 5000);
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

    ksock_set_group (self, "mygroup");
    ksock_set_subscribe (self, "test");
    ksock_connect (self, "localhost:9092");

    rd_kafka_message_t *msg = ksock_recv (self);
    zsock_handle_msg (self, msg, NULL);
    rd_kafka_message_destroy (msg);

    ksock_destroy (&self);
    //  @end
    printf ("OK\n");
}
