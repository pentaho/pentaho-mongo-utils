#!/usr/bin/expect
# ******************************************************************************
#
# Pentaho
#
# Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
#
# Use of this software is governed by the Business Source License included
# in the LICENSE.TXT file.
#
# Change Date: 2028-08-13
# ******************************************************************************

# simple script for retreiving and caching kerberos credentials for a user,
# as well as storing credentials in a specified keytab file.

# $1 -> principal
# $2 -> password
# $3 -> keytab file name

set user [lindex $argv 0]
set password [lindex $argv 1]
set keytabfile [lindex $argv 2]

# obtain and cache the tgt for the user
set timeout 120
spawn kinit  -V $user
expect "Password for $user:"
send "$password\r"

sleep 2

# --------------------------------------
# now generate a keytab file

spawn ktutil
expect "ktutil:"
send "addent -password -p $user -k 1 -e rc4-hmac\r"
expect "Password for $user:"
send "$password\r"
expect "ktutil:"
send "wkt $keytabfile\n"
sleep 2
send "exit"


