#
# file: t/use.t
#
# Last Update:		$Author: afrika $
# Update Date:		$Date: 2003/05/09 18:10:03 $
# Source File:		$Source: /home/cvsroot/tools/FileSystem/t/use.t,v $
# CVS/RCS Revision:	$Revision: 1.1 $
# Status:		$State: Exp $
# 
use strict;
use Test;

# use a BEGIN block so we print our plan before FileSystem is loaded
BEGIN { plan tests => 1 }

# load your module...
use DBIx::FileSystem;

# currently no usefull test available for an interactive shell
ok(1); # success
