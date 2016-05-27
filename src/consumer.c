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

struct _consumer_t {
    rd_kafka_t *rk;                                 // kafka handle 
    rd_kafka_conf_t *rk_conf;                       // kafka config
    rd_kafka_topic_conf_t *rk_topic_conf;           // kafka topic config
    rd_kafka_topic_partition_list_t *rk_topics;     // kafka topics
    char rk_errstr[512];                            // kafka error string
};


//  --------------------------------------------------------------------------
//  Create a new consumer

consumer_t *
consumer_new (void)
{
    consumer_t *self = (consumer_t *) zmalloc (sizeof (consumer_t));
    assert (self);

    self->rk = rd_kafka_new (RD_KAFKA_CONSUMER, self->rk_conf, self->rk_errstr, sizeof(self->rk_errstr));
    self->rk_conf = rd_kafka_conf_new ();
    self->rk_topic_conf = rd_kafka_topic_conf_new ();
    assert (self->rk);

    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the consumer

void
consumer_destroy (consumer_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        consumer_t *self = *self_p;
        if (self->rk_topics)
            rd_kafka_topic_partition_list_destroy (self->rk_topics);
       
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
consumer_test (bool verbose)
{
    printf (" * consumer: ");

    //  @selftest
    //  Simple create/destroy test
    consumer_t *self = consumer_new ();
    assert (self);
    consumer_destroy (&self);
    //  @end
    printf ("OK\n");
}
