/*  =========================================================================
    ksock - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.       
    This file is part of kafkaezq.                                     
                                                                       
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.           
    =========================================================================
*/

#ifndef KSOCK_H_INCLUDED
#define KSOCK_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new ksock
KAFKAEZQ_EXPORT ksock_t *
    ksock_new (void);

// Connect the ksock
KAFKAEZQ_EXPORT void
    ksock_connect (ksock_t *self, char *brokers);

//  Destroy the ksock
KAFKAEZQ_EXPORT void
    ksock_destroy (ksock_t **self_p);

//  Self test of this class
KAFKAEZQ_EXPORT void
    ksock_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
