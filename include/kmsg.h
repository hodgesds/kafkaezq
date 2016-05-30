/*  =========================================================================
    kmsg - class description

    Copyright (c) the Contributors as noted in the AUTHORS file.       
    This file is part of kafkaezq.                                     
                                                                       
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.           
    =========================================================================
*/

#ifndef KMSG_H_INCLUDED
#define KMSG_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

//  @interface
//  Create a new kmsg
KAFKAEZQ_EXPORT kmsg_t *
    kmsg_new (void);

//  Destroy the kmsg
KAFKAEZQ_EXPORT void
    kmsg_destroy (kmsg_t **self_p);

//  Self test of this class
KAFKAEZQ_EXPORT void
    kmsg_test (bool verbose);

//  @end

#ifdef __cplusplus
}
#endif

#endif
