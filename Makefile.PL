use ExtUtils::MakeMaker;
#
# Last Update:		$Author: marvin $
# Update Date:		$Date: 2003/04/09 11:07:10 $
# Source File:		$Source: /home/cvsroot/tools/FileSystem/Makefile.PL,v $
# CVS/RCS Revision:	$Revision: 1.1.1.1 $
# Status:		$State: Exp $
# 
# CVS/RCS Log:
# $Log: Makefile.PL,v $
# Revision 1.1.1.1  2003/04/09 11:07:10  marvin
# Imported Sources
#
#
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'	=> 'DBIx::FileSystem',
    'VERSION_FROM' => 'FileSystem.pm', # finds $VERSION
    'PREREQ_PM'    => { 
			 'DBI' => 1,
		      },

);
