/*  =========================================================================
    kmsg - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.       
    This file is part of kafkaezq.                                     
                                                                       
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.           
    =========================================================================
*/

/*
@header
    kmsg - 
@discuss
@end
*/

#include "kafkaezq_classes.h"

//  Structure of our class

struct _kmsg_t {
    rd_kafka_message_t *msg;
};


//  --------------------------------------------------------------------------
//  Create a new kmsg

kmsg_t *
kmsg_new (void)
{
    kmsg_t *self = (kmsg_t *) zmalloc (sizeof (kmsg_t));
    assert (self);
    //  Initialize class properties here
    return self;
}


//  --------------------------------------------------------------------------
//  Destroy the kmsg

void
kmsg_destroy (kmsg_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        kmsg_t *self = *self_p;

        if (self->msg)
            rd_kafka_message_destroy (self->msg); 
        
        //  Free object itself
        free (self);
        *self_p = NULL;
    }
}

//  --------------------------------------------------------------------------
//  Self test of this class

void
kmsg_test (bool verbose)
{
    printf (" * kmsg: ");

    //  @selftest
    //  Simple create/destroy test
    kmsg_t *self = kmsg_new ();
    assert (self);
    kmsg_destroy (&self);
    //  @end
    printf ("OK\n");
}
