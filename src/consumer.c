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
    int filler;     //  Declare class properties here
};


//  --------------------------------------------------------------------------
//  Create a new consumer

consumer_t *
consumer_new (void)
{
    consumer_t *self = (consumer_t *) zmalloc (sizeof (consumer_t));
    assert (self);
    //  Initialize class properties here
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
        //  Free class properties here
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
