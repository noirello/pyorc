#ifndef VERGUARD_H
#define VERGUARD_H

#if ORC_VERSION_MINOR > 255 || ORC_VERSION_PATCH > 255
#error "ORC version number component is higher than 255, version guard macro will fail"
#endif

#define ORC_VERSION_AT_LEAST(ma, mi, pa) \
    (((ORC_VERSION_MAJOR)<<16)+((ORC_VERSION_MINOR)<<8)+(ORC_VERSION_PATCH) >= (((ma)<<16)+((mi)<<8)+(pa)) ? 1 : 0)

#endif

