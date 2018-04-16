/*  =========================================================================
    cidp.h - CIDP wrapper

    -------------------------------------------------------------------------
    Copyright (c) 1991-2012 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of the Majordomo Project: http://majordomo.zeromq.org.

    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
    =========================================================================
*/


//
//  idp.h
//  Majordomo Protocol definitions
//
#ifndef __IDP_H_INCLUDED__
#define __IDP_H_INCLUDED__

//  This is the version of IDP/Client we implement
#define IDPC_CLIENT         "IDPC01"

//  This is the version of IDP/Worker we implement
#define IDPW_WORKER         "IDPW01"

//  IDP/Server commands, as strings
#define IDPW_READY          "\001"
#define IDPW_REQUEST        "\002"
#define IDPW_REPLY          "\003"
#define IDPW_HEARTBEAT      "\004"
#define IDPW_DISCONNECT     "\005"

static char const *idps_commands [] = {
    nullptr, "READY", "REQUEST", "REPLY", "HEARTBEAT", "DISCONNECT"
};

#endif

