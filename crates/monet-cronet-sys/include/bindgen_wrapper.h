// Bindgen entry point only: cronet.idl_c.h uses C99 `bool` without including
// <stdbool.h> (its native consumers are C++). Not part of the vendored API.
#include <stdbool.h>

#include "cronet_c.h"
