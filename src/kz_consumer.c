/*  =========================================================================
    consumer - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.       
    This file is part of kafkaezq.                                     
                                                                       
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.           
    =========================================================================
*/

/*
@header
    consumer - 
@discuss
@end
*/

#include "kafkaezq_classes.h"

//  Structure of our class

struct _kz_consumer_t {
    rd_kafka_t *rk;                                 // kafka handle 
    rd_kafka_conf_t *rk_conf;                       // kafka conf
    rd_kafka_topic_conf_t *rk_topic_conf;           // kafka topic conf
    rd_kafka_topic_t *rk_topic;                     // kafka topic
    rd_kafka_message_t **rk_messages;               // kafka messages
    int rk_batch_size;                              // kafka batch size
    char rk_errstr[512];                            // kafka error string
};

static void 
s_err_callback (rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
    const char *errstr = rd_kafka_err2str (rd_kafka_errno2err(err));
	printf("%% ERROR CALLBACK: %s: %s: %s\n",
	       rd_kafka_name(rk), errstr, reason);
}

//  --------------------------------------------------------------------------
//  Create a new consumer

kz_consumer_t *
kz_consumer_new ()
{
    kz_consumer_t *self = (kz_consumer_t *) zmalloc (sizeof (kz_consumer_t));
    assert (self);

    self->rk_messages = NULL;
    self->rk_batch_size = 0;
    self->rk_conf = rd_kafka_conf_new ();
    self->rk = rd_kafka_new (RD_KAFKA_CONSUMER, self->rk_conf, self->rk_errstr, sizeof(self->rk_errstr));
    assert (self->rk);

    return self;
}

//  --------------------------------------------------------------------------
//  Set error callback

void
kz_consumer_set_error_callback (kz_consumer_t *self, void (*error_cb) (rd_kafka_t *rk, int err, const char *reason, void *opaque))
{
    rd_kafka_conf_set_error_cb (self->rk_conf, error_cb);
}

//  --------------------------------------------------------------------------
//  Destroy the consumer

void
kz_consumer_destroy (kz_consumer_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        kz_consumer_t *self = *self_p;
        if (self->rk) 
            rd_kafka_destroy (self->rk);

        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

//  --------------------------------------------------------------------------
//  Self test of this class

void
kz_consumer_test (bool verbose)
{
    printf (" * consumer: ");

    //  @selftest
    //  Simple create/destroy test
    kz_consumer_t *self = kz_consumer_new ();
    assert (self);

    kz_consumer_set_error_callback (self, s_err_callback);
    kz_consumer_destroy (&self);
    //  @end
    printf ("OK\n");
}
