#
# DBIx::FileSystem;
#
# Manage database tables with a simulated filesystem shell environment
#
# Mar 2003    Alexander Haderer
#
# License:
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
# Last Update:		$Author: marvin $
# Update Date:		$Date: 2003/07/17 17:32:24 $
# Source File:		$Source: /home/cvsroot/tools/FileSystem/FileSystem.pm,v $
# CVS/RCS Revision:	$Revision: 1.8 $
# Status:		$State: Exp $
# 
# CVS/RCS Log:
# $Log: FileSystem.pm,v $
# Revision 1.8  2003/07/17 17:32:24  marvin
# pawactl custom command example hello --> count
#
# Revision 1.7  2003/07/16 18:35:45  marvin
# updated doku
#
# Revision 1.6  2003/07/16 18:32:33  marvin
# Added custom commands
# valok and rmcheck now get $dbh param
#
# Revision 1.5  2003/07/11 17:59:17  marvin
# valok now gets additional parameter: a hashref of all values read from file
#
# Revision 1.4  2003/07/11 15:40:56  marvin
# multiline descriptions, cp now checks filename
#
# Revision 1.3  2003/05/09 18:09:59  afrika
# rename t/dummy.t to t/use.t
#
# Revision 1.2  2003/05/08 18:26:06  afrika
# added t/dummy.t dummy test
#
# Revision 1.1.1.1  2003/04/09 11:07:10  marvin
# Imported Sources
#
#

package DBIx::FileSystem;

use strict;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);
use Exporter;

$DBIx::FileSystem::VERSION = '1.06';

@ISA = qw( Exporter );
@EXPORT_OK = qw(
	     &recreatedb
	     &mainloop
	     );

use vars qw( $OUT $vwd $dbh );

use DBI;
use Term::ReadLine;
use POSIX qw{tmpnam};
use Fcntl;


########################################################################
# c o m m a n d s
########################################################################
my %commands =
  ('cd'=> 	{ func => \&com_cd,
		  doc => "change to directory: 'cd DIR'" },
   'help' => 	{ func => \&com_help,
		  doc => "display help text: 'help [command]'" },
   'quit' => 	{ func => \&com_quit,
		  doc => "quit it" },
   'ls' => 	{ func => \&com_ls,
		  doc => "list dirs and files" },
   'ld'=> 	{ func => \&com_ld,
		  doc => "list long dirs and files with comments" },
   'll' => 	{ func => \&com_ll,
		  doc => "list long files with comments" },
   'rm' => 	{ func => \&com_rm,
		  doc => "remove file: 'rm FILE'" },
   'cp' => 	{ func => \&com_cp,
		  doc => "copy file: 'cp OLD NEW'" },
   'cat' => 	{ func => \&com_cat,
		  doc => "show contents of a file: 'cat FILE'" },
   'sum' => 	{ func => \&com_sum,
		  doc => "show summary of a file: 'sum FILE'" },
   'vi' => 	{ func => \&com_vi,
		  doc => "edit/create a file: 'vi FILE'" },
   'ver' => 	{ func => \&com_ver,
		  doc => "show version" },
   'wrefs' => 	{ func => \&com_wref,
		  doc => "show who references a file: 'wrefs FILE'" },
  );


########################################################################
# C o n s t a n t s
########################################################################

# for ls output
my $NUM_LS_COL = 4;
my $LS_COL_WIDTH = 16;
my $EDITOR = $ENV{EDITOR};
$EDITOR = "/usr/bin/vi" unless $EDITOR;


########################################################################
# m a i n
#
# input:
#  vdirs:	reference to vdir hash
#  PRG:		program name for the shell-program
#  VERSION	four digit version string for program/database version
#  DBHOST	DBI connect string for the database
#  DBUSER       database user
#
# returns nothing
########################################################################

my $vdirs;	# reference to vdir hash
# $vwd ;	# current virtual working directory (exported)

# my $dbh;	# database handle (exported)
my $term;
# $OUT;		# the stdout (exported)

my $DBHOST;	# DBI database connect string
my $DBUSER;	# DBI database user
my $DBPWD;	# DBI password
my $VERSION;

my $PRG;	# program name of the shell


sub mainloop(\%$$$$$\%) {

  my $customcmds;
  ($vdirs,$PRG,$VERSION,$DBHOST,$DBUSER,$DBPWD,$customcmds) = @_;

  # merge custom commands, if any
  if( defined $customcmds ) {
    foreach my $cucmd (keys (%{$customcmds} ) ) {
      if( defined $commands{$cucmd} ) {
	die "$PRG: redefinition of command '$cucmd' by customcommands";
      }
      unless( defined $customcmds->{$cucmd}{func} ) {
	die "$PRG: customcommand '$cucmd': elem func not set";
      }
      unless( defined $customcmds->{$cucmd}{doc} ) {
	die "$PRG: customcommand '$cucmd': elem doc not set";
      }
      $commands{$cucmd} = $customcmds->{$cucmd};
    }
  }

  # connect to db
  ($dbh = DBI->connect( $DBHOST, $DBUSER, $DBPWD,
     {ChopBlanks => 1, AutoCommit => 1, PrintError => 0})) 
     || die "$PRG: connect to '$DBHOST' failed:\n", $DBI::errstr;

  # check vdirs
  if( check_vdirs_struct() ) {
    $dbh->disconnect || die "$PRG: Disconnect failed. Reason: ", $DBI::errstr;
    die "$PRG: check 'vdirs' structure in $PRG\n";
  }

  # check database
  if( check_db_tables() ) {
    $dbh->disconnect || die "$PRG: Disconnect failed. Reason: ", $DBI::errstr;
    die "$PRG: database wrong: run '$PRG recreatedb' to recreate tables\n";
  }

  # readline settings
  $term = new Term::ReadLine 'dbshell console';
  $OUT = $term->OUT || \*STDOUT;
  $term->ornaments( 0 );
  $term->Attribs->{attempted_completion_function} = \&dbshell_completion;

  my $line;	# command line
  my $cmd;	# the command 
  my @arg;	# the command's parameters

  my $prompttemplate = "$PRG (/%s): ";
  my $prompt = sprintf( $prompttemplate, $vwd );

  # the loop
  while ( defined ($line = $term->readline($prompt)) ) {
    # remove whitespace
    $line =~ s/^\s*//;
    $line =~ s/\s*//;
    ($cmd, @arg ) = split( ' ', $line );
    next unless defined $cmd;
    
    my $command = $commands{$cmd};
    if( defined $command ) {
      last if &{$command->{func}}( @arg );
    }else{
      print $OUT "unknown command '$cmd', try 'help'\n";
    }
    $prompt = sprintf( $prompttemplate, $vwd );
  }
  $dbh->disconnect || die "$PRG: Disconnect failed. Reason: ", $DBI::errstr;
  return;
}


sub recreatedb(\%$$$$$) {

  ($vdirs,$PRG,$VERSION,$DBHOST,$DBUSER,$DBPWD) = @_;

  # connect to db
  ($dbh = DBI->connect( $DBHOST, $DBUSER, $DBPWD,
     {ChopBlanks => 1, AutoCommit => 1, PrintError => 0})) 
     || die "$PRG: connect to '$DBHOST' failed:\n", $DBI::errstr;

  # check vdirs
  if( check_vdirs_struct() ) {
    die "$PRG: check 'vdirs' structure in $PRG\n";
  }

  recreate_db_tables();

  $dbh->disconnect || die "$PRG: Disconnect failed. Reason: ", $DBI::errstr;
  return;
}


########################################################################
# c o m m a n d   f u n c t i o n s
########################################################################

########################################################################
# com_help()
#
sub com_help() {
  my $arg = shift;
  if( defined $arg ) {
    if( defined $commands{$arg} ) {
      print $OUT "$arg\t$commands{$arg}->{doc}\n";
    }else{
      print $OUT "no help for '$arg'\n";
    }
  }else{
    foreach my $i (sort keys(%commands) ) {
      print $OUT "$i\t$commands{$i}->{doc}\n";
    }
  }
  return 0;
}

########################################################################
# com_ls()
#
sub com_ls() {
  my @files;
  my $i;

  my $x = shift;
  if( defined $x ) {
    print $OUT "ls: usage: $commands{ls}->{doc}\n";
    return 0;
  }

  # get dirs
  foreach $i (sort keys(%{$vdirs}) ) {
    push @files, "($i)";
  }

  # get files
  if( length($vwd) ) {
    my $st;
    my $col = $vdirs->{$vwd}{fnamcol};
    $st = $dbh->prepare("select $col from $vwd order by $col");
    unless( $st ) {
      print $OUT "$PRG: can't prepare ls query '$vwd':\n  $DBI::errstr\n";
      return 0;
    }
    unless( $st->execute() ) {
      print $OUT "$PRG: can't exec ls query '$vwd':\n  $DBI::errstr\n";
      return 0;
    }
    while( $i = $st->fetchrow_array() ) {
      push @files, "$i";
    }
    $st->finish();
  }

  # show it
  my $numrow = int( $#files / $NUM_LS_COL ) + 1;
  my $r = 0;
  my $c = 0;
  my $placeh = $LS_COL_WIDTH - 2;
  for( $r=0; $r<$numrow; $r++ ) {
    for( $c=0; $c<$NUM_LS_COL; $c++ ) {
      $i = $c*$numrow+$r;
      printf $OUT "%-${placeh}s  ", $files[$i] if $i <= $#files;
    }
    print $OUT "\n";
  }
  return 0;
}

########################################################################
# com_ld()
#
sub com_ld() {
  my @files;
  my @com;	# comments
  my $i;
  my $x = shift;
  if( defined $x ) {
    print $OUT "ls: usage: $commands{ld}->{doc}\n";
    return 0;
  }

  # get dirs
  foreach $i (sort keys(%{$vdirs}) ) {
    push @files, "($i)";
    push @com, $vdirs->{$i}{desc};
  }

  # show it
  my $maxlen = 0;
  foreach $i (@files) {
    if( length($i) > $maxlen ) {$maxlen = length($i); }
  }

  for( $i=0; $i<=$#files; $i++ ) {
    printf $OUT "%-${maxlen}s| %s\n", $files[$i], $com[$i];
  }
  print $OUT "\n";
  com_ll();
  return 0;
}

########################################################################
# com_ll()
#
sub com_ll() {
  my @files;
  my @com;	# comments
  my $i;
  my $c;

  my $x = shift;
  if( defined $x ) {
    print $OUT "ls: usage: $commands{ll}->{doc}\n";
    return 0;
  }

  # get files
  if( defined $vdirs->{$vwd}{comcol} ) {
    my $comcol = $vdirs->{$vwd}{comcol};
    my $col = $vdirs->{$vwd}{fnamcol};
    my $st;
    $st = $dbh->prepare("select $col, $comcol from $vwd order by $col");
    unless( $st ) {
      print $OUT "$PRG: can't prepare ll query '$vwd':\n  $DBI::errstr\n";
      return 0;
    }
    unless( $st->execute() ) {
      print $OUT "$PRG: can't exec ll query '$vwd':\n  $DBI::errstr\n";
      return 0;
    }
    while( ($i,$c) = $st->fetchrow_array() ) {
      $c = "" unless defined $c;
      push @files, "$i";
      push @com, "$c";
    }
    $st->finish();
  }else{
    my $st;
    my $col = $vdirs->{$vwd}{fnamcol};
    $st = $dbh->prepare("select $col from $vwd order by $col");
    unless( $st ) {
      print $OUT "$PRG: can't prepare ls query '$vwd':\n  $DBI::errstr\n";
      return 0;
    }
    unless( $st->execute() ) {
      print $OUT "$PRG: can't exec ls query '$vwd':\n  $DBI::errstr\n";
      return 0;
    }
    while( $i = $st->fetchrow_array() ) {
      push @files, "$i";
      push @com, "";
    }
    $st->finish();
  }

  # show it
  my $maxlen = 0;
  foreach $i (@files) {
    if( length($i) > $maxlen ) {$maxlen = length($i); }
  }

  for( $i=0; $i<=$#files; $i++ ) {
    printf $OUT "%-${maxlen}s| %s\n", $files[$i], $com[$i];
  }
  return 0;
}

########################################################################
# com_cd()
#
sub com_cd() {
  my ($arg,$x) = @_;
  if( defined $arg and !defined $x) {
     if( exists $vdirs->{$arg} ) {
       $vwd = "$arg";
     }else{
       print $OUT "no such directory '$arg'\n";
     }
  }else{
    print $OUT "cd: usage: $commands{cd}->{doc}\n";
  }
  return 0;
}


########################################################################
# com_quit()
#
sub com_quit() {
  return 1;
}

########################################################################
# com_ver()
#
sub com_ver() {
  print $OUT "   $PRG: config database editor\n";
  print $OUT "   program version $VERSION\n";
  return 0;
}

########################################################################
# com_new()
# no longer supported
#
sub com_new() {
  my $r;
  my $arg = shift;
  if( defined $arg ) {
    if( exists $vdirs->{$vwd} and $vdirs->{$vwd}->{edit} ) {
      my $fnc = $vdirs->{$vwd}{fnamcol};
      if( (length($arg)<=$vdirs->{$vwd}{cols}{$fnc}{len}) and !($arg=~/\W+/)) {
	$r = $dbh->do( "insert into $vwd ($fnc) values ('$arg')");
	if( !defined $r ) {
	  print $OUT "new: couldn't create: database error:\n$DBI::errstr\n";
	}elsif( $r==0 ) {
	  print $OUT "new: couldn't create\n";
	}
      }else{
	print $OUT "new: error: illegal or to long filename '$arg'\n";
      }
    }else{
      print $OUT "new: error: read only directory '/$vwd'\n";
    }
  }else{
    print $OUT "new: usage: $commands{new}->{doc}\n";
  }
  return 0;
}

########################################################################
# com_rm()
#
sub com_rm() {
  my $r;
  my ($arg,$x) = @_;
  if( defined $arg and !defined $x ) {
    if( exists $vdirs->{$vwd} and $vdirs->{$vwd}{edit} ) {
      if( $vdirs->{$vwd}{defaultfile} and $vdirs->{$vwd}{defaultfile} eq $arg ) {
	print $OUT "rm: error: cannot remove default file '$arg'\n";
      }else{
	my @reffiles = get_who_refs_me( $vwd, $arg );
	if( $#reffiles == -1 ) {
	  my $rmerr;
	  if( exists $vdirs->{$vwd}{rmcheck} ) {
	    $rmerr = &{$vdirs->{$vwd}->{rmcheck}}( $vwd, $arg, $dbh);
	  }
	  if( defined $rmerr ) {
	    print $OUT "rm: cannot remove: $rmerr\n";
	  }else{
	    my $fnc = $vdirs->{$vwd}{fnamcol};
	    $r = $dbh->do( "delete from $vwd where $fnc='$arg'");
	    if( !defined $r ) { 
	      print $OUT "rm: database error:\n$DBI::errstr\n";
	    }elsif( $r==0 ) { 
	      print $OUT "rm: no such file '$arg'\n";
	    }
	  }
	}else{
	  print $OUT "rm: cannot remove: file '$arg' referenced by:\n  ";
	  print $OUT join( "\n  ", @reffiles );
	  print $OUT "\n";
	}
      }
    }else{
      print $OUT "rm: error: read only directory '/$vwd'\n";
    }
  }else{
    print $OUT "rm: usage: $commands{rm}{doc}\n";
  }
  return 0;
}


########################################################################
# com_cp()
#
sub com_cp() {
  my $r;
  my ($old,$new,$x) = @_;
  if( defined $old and defined $new and !defined $x) {
    if( exists $vdirs->{$vwd} and $vdirs->{$vwd}{edit} ) {
      my $fnc = $vdirs->{$vwd}{fnamcol};
      if( (length($new)<=$vdirs->{$vwd}{cols}{$fnc}{len}) and !($new=~/\W+/)) {
	my $fnc = $vdirs->{$vwd}{fnamcol};
	my $insert = "insert into $vwd (";
	my $select = "select ";
	my $cols   = $vdirs->{$vwd}{cols};
	foreach my $col (sort keys(%{$cols}) ) {
	  $insert .= "$col,";
	  if( $col eq $fnc ) {
	    $select .= "'$new',";
	  }elsif( exists $vdirs->{$vwd}{cols}{$col}{delcp} ) {
	    $select .= "NULL,";
	  }else{
	    $select .= "$col,";
	  }
	}
	chop $insert;
	chop $select;
	$insert .= ")";
	$select .= " from $vwd where $fnc='$old'";
	$r = $dbh->do( "$insert $select");
	if( !defined $r or $r!=1 ) { 
	  print "cp: error: no file '$old' or file '$new' exists\n"; 
	}
      }else{
	print $OUT "cp: error: illegal or to long filename '$new'\n";
      }
    }else{
      print $OUT "cp: error: read only directory '/$vwd'\n";
    }
  }else{
    print $OUT "cp: usage: $commands{cp}{doc}\n";
  }
  return 0;
}


########################################################################
# com_sum()
#
sub com_sum() {
  my ($arg,$x) = @_;

  if( defined $arg and !defined $x ) {
    if( exists $vdirs->{$vwd} ) {
      if( print_file( $OUT, $arg, 0 ) == 1 ) {
	print $OUT "sum: no such file '$arg'\n";
      }
    }else{
      print $OUT "sum: no such file '$arg'\n";
    }
  }else{
    print $OUT "sum: usage: $commands{sum}{doc}\n";
  }
  return 0;
}

########################################################################
# com_cat()
#
sub com_cat() {
  my ($arg,$x) = @_;

  if( defined $arg and !defined $x ) {
    if( exists $vdirs->{$vwd} ) {
      if( print_file( $OUT, $arg, 1 ) == 1 ) {
	print $OUT "cat: no such file '$arg'\n";
      }
    }else{
      print $OUT "cat: no such file '$arg'\n";
    }
  }else{
    print $OUT "cat: usage: $commands{cat}{doc}\n";
  }
  return 0;
}

########################################################################
# com_vi()
#
sub com_vi() {
  my ($arg,$x) = @_;
  my $tmpf;
  my $tmpf_mtime;
  my $r;	# 0: file printed exists / create update SQL string
  	;	# 1: file printed did not exist / create insert SQL string
  my $err;
  my $sql;
  my $ln = 1;		# line number where editor starts

  if( defined $arg and !defined $x ) {
    if( exists $vdirs->{$vwd} and $vdirs->{$vwd}{edit} ) {
      my $fnc = $vdirs->{$vwd}{fnamcol};
      if( (length($arg)<=$vdirs->{$vwd}{cols}{$fnc}{len}) and !($arg=~/\W+/)) {
	while( 1 ) { $tmpf = tmpnam();
		     sysopen( FN, $tmpf, O_RDWR | O_CREAT | O_EXCL ) && last; }
	$r = print_file( \*FN, $arg, 2 );
	close( FN );
	$tmpf_mtime = (stat $tmpf)[9];	# remember mtime of tempfile
	if( $r==0 or $r==1 ) {
	  while( 1 ) {
	    system( "$EDITOR +$ln $tmpf" );
	    ($ln,$err,$sql) = create_sql_from_file( $tmpf, $vwd, $arg, $r );
	    if( defined $err ) {
	      my $inp = want_to_edit_again( $err );
	      next if $inp eq 'y';
	      last if $inp eq 'n';
	    }
#	    print $OUT ">>>$sql<<<\n";	#########
	    if( length($sql) and $tmpf_mtime != (stat $tmpf)[9] ) {
	      my $res = $dbh->do( $sql );
	      if( !defined $res ) {
		my $inp=want_to_edit_again( "save to database:\n$DBI::errstr");
		if($inp eq 'y') { $ln = 1; next; }
	      }elsif( $res == 0 ) {
		print $OUT "\n\n\n\n\nvi: nothing saved\n";
	      }
	    }else{
	      print $OUT "\n\n\n\n\nvi: nothing saved\n";
	    }
	    last;
	  }
	}else{
	  print $OUT "vi: no such file '$arg'\n";
	}
	unlink( $tmpf );
      }else{
	print $OUT "vi: error: illegal or to long filename '$arg'\n";
      }
    }else{
      print $OUT "vi: error: read only directory '/$vwd'\n";
    }
  }else{
    print $OUT "vi: usage: $commands{vi}{doc}\n";
  }
  return 0;
}

########################################################################
# com_wref()
#
sub com_wref() {
  my $r;
  my ($arg,$x) = @_;
  if( defined $arg and !defined $x ) {
    if( exists $vdirs->{$vwd} ) {
      my @reffiles = get_who_refs_me( $vwd, $arg );
      if( $#reffiles > -1 ) {
	print $OUT join( "\n", @reffiles );
	print $OUT "\n";
      }else{
	print $OUT "wrefs: no one references '$arg'\n";
      }
    }else{
      print $OUT "wrefs: no such file '$arg'\n";
    }
  }else{
    print $OUT "wrefs: usage: $commands{wrefs}{doc}\n";
  }
  return 0;
}






########################################################################
# c o m p l e t i o n 
########################################################################

# from p5-ReadLine example 'FileManager'

# Attempt to complete on the contents of TEXT.  START and END bound
# the region of rl_line_buffer that contains the word to complete.
# TEXT is the word to complete.  We can use the entire contents of
# rl_line_buffer in case we want to do some simple parsing.  Return
# the array of matches, or NULL if there aren't any.
sub dbshell_completion {
  my ($text, $line, $start, $end) = @_;
  
  my @matches = ();

  # If this word is at the start of the line, then it is a command
  # to complete.  Otherwise it is the name of a file in the current
  # directory.
  if ($start == 0) {
    @matches = $term->completion_matches($text, \&command_generator);
  }elsif($line =~ /^cd\s.*/ ) {
    @matches = $term->completion_matches($text, \&vdir_generator);
  }else{
    @matches = $term->completion_matches($text, \&vfile_generator);
  }

  return @matches;
}

# from p5-ReadLine example 'FileManager'
# Generator function for command completion.  STATE lets us know
# whether to start from scratch; without any state (i.e. STATE == 0),
# then we start at the top of the list.

## Term::ReadLine::Gnu has list_completion_function similar with this
## function.  I defined new one to be compared with original C version.
{
  my $list_index;
  my @name;

  sub command_generator {
    my ($text, $state) = @_;
    $text =~ s/\./\\\./g;
    $text =~ s/\*/\\\*/g;
    $text =~ s/\[/\\\[/g;
    $text =~ s/\]/\\\]/g;
    $text =~ s/\$/\\\$/g;
    $text =~ s/\^/\\\^/g;

    # If this is a new word to complete, initialize now.  This
    # includes saving the length of TEXT for efficiency, and
    # initializing the index variable to 0.
    unless ($state) {
      $list_index = 0;
      @name = keys(%commands);
    }

    # Return the next name which partially matches from the
    # command list.
    while ($list_index <= $#name) {
      $list_index++;
      return $name[$list_index - 1]
	if ($name[$list_index - 1] =~ /^$text/);
    }
    # If no names matched, then return NULL.
    return undef;
  }
}

{
  my $list_index;
  my @name;

  sub vdir_generator {
    my ($text, $state) = @_;
    $text =~ tr/a-zA-Z0-9_\///cd;
    $text =~ s/\./\\\./g;
    $text =~ s/\*/\\\*/g;
    $text =~ s/\[/\\\[/g;
    $text =~ s/\]/\\\]/g;
    $text =~ s/\$/\\\$/g;
    $text =~ s/\^/\\\^/g;
    
    # If this is a new word to complete, initialize now.  This
    # includes saving the length of TEXT for efficiency, and
    # initializing the index variable to 0.
    unless ($state) {
      $list_index = 0;
      @name = keys(%{$vdirs});
    }

    # Return the next name which partially matches 
    while ($list_index <= $#name) {
      $list_index++;
      return $name[$list_index - 1]
	if ($name[$list_index - 1] =~ /^$text/);
    }
    # If no names matched, then return NULL.
    return undef;
  }
}

{
  my $list_index;
  my @name;

  sub vfile_generator {
    my ($text, $state) = @_;
    $text =~ tr/a-zA-Z0-9_\///cd;
    $text =~ s/\./\\\./g;
    $text =~ s/\*/\\\*/g;
    $text =~ s/\[/\\\[/g;
    $text =~ s/\]/\\\]/g;
    $text =~ s/\$/\\\$/g;
    $text =~ s/\^/\\\^/g;
    
    unless ($state) {
      undef @name;
      $list_index = 0;
      my $st;
      my $col = $vdirs->{$vwd}{fnamcol};
      $st = $dbh->prepare("select $col from $vwd order by $col");
      unless( $st ) {
	print $OUT "$PRG: prep completion query '$vwd':\n  $DBI::errstr\n";
	return undef;
      }
      unless( $st->execute() ) {
	print $OUT "$PRG: exec completion query '$vwd':\n  $DBI::errstr\n";
	return undef;
      }
      my $i;
      while( $i = $st->fetchrow_array() ) {
	push @name, $i;
      }
      $st->finish();
    }

    # Return the next name which partially matches 
    while ($list_index <= $#name) {
      $list_index++;
      return $name[$list_index - 1]
	if ($name[$list_index - 1] =~ /^$text/);
    }
    # If no names matched, then return NULL.
    return undef;
  }
}

########################################################################
# c h e c k i n g  &  c r e a t i o n
########################################################################

########################################################################
# check_vdirs_struct()
#
sub check_vdirs_struct() {
  my $pre = "fcsictl: internal error: vdirs structure:\n ";
  foreach my $dir (keys(%{$vdirs}) ) {
    # init refby: 
    # a hash holding the dir (key) and list of columns (value) this dir 
    # is referenced by. Will be set up 57 lines later (# setup refby)
    $vdirs->{$dir}->{refby} = {};
  }

  foreach my $dir (sort keys(%{$vdirs}) ) {
    $vwd = $dir unless defined $vwd; # set $vwd to alphabetic first dir 

    unless( defined $vdirs->{$dir}->{desc}) {
      print "$pre dir '$dir': 'desc' missing\n";
      return 1;
    }
    unless( defined $vdirs->{$dir}->{edit}) {
      print "$pre dir '$dir': 'edit' missing\n";
      return 1;
    }

    unless( defined $vdirs->{$dir}->{cols}) {
      print "$pre dir '$dir': 'cols' missing\n";
      return 1;
    }

    unless( defined $vdirs->{$dir}->{refby}) {
      print "$pre dir '$dir': 'refby' missing \n";
      return 1;
    }

    my $fnamcol = $vdirs->{$dir}{fnamcol};
    unless( defined $fnamcol) {
      print "$pre dir '$dir': 'fnamcol' missing\n";
      return 1;
    }
    unless( defined $vdirs->{$dir}{cols}{$fnamcol} ) {
      print "$pre dir '$dir', fnamcol set to '$fnamcol', but column missing\n";
      return 1;
    }
    if( $vdirs->{$dir}{cols}{$fnamcol}{type} ne 'char' ) {
      print "$pre dir '$dir', fnamcol-column '$fnamcol' type must be 'char'\n";
      return 1;
    }
    if( $vdirs->{$dir}{edit} == 1 ) {
      unless( defined $vdirs->{$dir}{cols}{$fnamcol}{len} ) {
	print "$pre dir '$dir', fnamcol-column '$fnamcol': missing 'len'\n";
	return 1;
      }
    }
    if( $vdirs->{$dir}{cols}{$fnamcol}{len} + 2 > $LS_COL_WIDTH ) {
      my $maxlen = $LS_COL_WIDTH - 2;
      print "$pre dir '$dir', fnamcol-column '$fnamcol' len > $maxlen\n";
      return 1;
    }

    my $comcol = $vdirs->{$dir}{comcol};
    if( defined $comcol) {
      unless( defined $vdirs->{$dir}{cols}{$comcol} ) {
	print "$pre dir '$dir', comcol set to '$comcol', but column missing\n";
	return 1;
      }
      if( $vdirs->{$dir}{cols}{$comcol}{type} ne 'char' ) {
	print "$pre dir '$dir', comcol-column '$comcol' type must be 'char'\n";
	return 1;
      }
      unless( defined $vdirs->{$dir}{cols}{$comcol}{len} ) {
	print "$pre dir '$dir', comcol-column '$comcol': missing 'len'\n";
	return 1;
      }
    }

    my $cols = $vdirs->{$dir}{cols};
    foreach my $col (keys(%{$cols} )) {
      unless( defined $cols->{$col}{type} || defined $cols->{$col}{ref} ) {
	print "$pre dir '$dir', column '$col', either 'type' or 'ref' must be set\n";
	return 1;
      }
      if( defined $cols->{$col}{ref} and !defined $vdirs->{$cols->{$col}{ref}}){
	print "$pre dir '$dir', column '$col', elem 'ref': no dir '$cols->{$col}{ref}'\n"; 
	return 1;
      }
      # setup refby
      if( defined $cols->{$col}{ref} ) {
	push @{$vdirs->{$cols->{$col}{ref}}{refby}{$dir} }, $col;
      }

      if( defined $cols->{$col}{type} and $vdirs->{$dir}{edit}==1) {
	if( $cols->{$col}{type} ne 'char' and
	    $cols->{$col}{type} ne 'int' and
	    $cols->{$col}{type} ne 'smallint' )
	{
	  print "$pre dir '$dir', column '$col', type must be one of char/int/smallint when edit=1\n"; 
	  return 1;
	}
      }

      unless( defined $cols->{$col}{var} ) {
	print "$pre dir '$dir', column '$col', missing elem 'var'\n"; 
	return 1;
      }
      unless( defined $cols->{$col}{desc} ) {
	print "$pre dir '$dir', column '$col', missing elem 'desc'\n"; 
	return 1;
      }
      unless( defined $cols->{$col}{pos} ) {
	print "$pre dir '$dir', column '$col', missing elem 'pos'\n"; 
	return 1;
      }
    }
  }
  return 0;
}

########################################################################
# check_db_tables()
#
sub check_db_tables() {
  my $st;

  # check version no of db tables
  $st = $dbh->prepare("select value from tablestatus where tag='version' ");
  unless( $st ) {
    print "$PRG: can't prepare query 'version':\n  $DBI::errstr\n";
    return 1;
  }
  unless( $st->execute() ) {
    print "$PRG: can't execute query 'version':\n  $DBI::errstr\n";
    return 1;
  }
  
  my ($dbversion) = $st->fetchrow_array();
  unless( $dbversion ) {
    print "$PRG: can't query db: table version\n";
    return 1;
  }
  
  $st->finish();
  if( $VERSION ne $dbversion ) {
    print 
      "$PRG: version mismatch: $PRG='$VERSION' dbtables='$dbversion'\n";
    return 1;
  }

  # check (existence) of other tables
  foreach my $i (sort keys(%{$vdirs}) ) {
    $st = $dbh->prepare("select * from $i limit 1");
    unless( $st ) {
      print "$PRG: can't prepare query '$i':\n  $DBI::errstr\n";
      return 1;
    }
    unless( $st->execute() ) {
      print "$PRG: can't execute query '$i':\n  $DBI::errstr\n";
      return 1;
    }
    my @dummy = $st->fetchrow_array();
    $st->finish();
  }
  return 0;	# all Ok
}



########################################################################
# recreate_db_tables();
#
sub recreate_db_tables() {
  my $r;
  $dbh->do( "drop table tablestatus" );
  $r = $dbh->do( 
     qq{ create table tablestatus ("tag" char(16), 
				   "value" char(16) PRIMARY KEY) } );
  unless( $r ) {
    print "$PRG: create table tablestatus:\n  $DBI::errstr\n";
    return;
  }
  $r = $dbh->do( 
     qq{ insert into tablestatus (tag, value) values ('version','$VERSION' )});
  unless( $r ) {
    print "$PRG: insert version into tablestatus:\n  $DBI::errstr\n";
    return;
  }

  # recreate other tables
  foreach my $tab (sort keys(%{$vdirs}) ) {
    $dbh->do( "drop table $tab" );
    my $create = "create table $tab (";
    my $cols   = $vdirs->{$tab}{cols};
    foreach my $col (keys(%{$cols}) ) {
      if( defined $cols->{$col}{ref} ) {
	my $rdir = $cols->{$col}{ref};
	my $rfnc = $vdirs->{$rdir}{fnamcol};
	if( defined $vdirs->{$rdir}{cols}{$rfnc}{len} ) {
	  $create .= "$col $vdirs->{$rdir}{cols}{$rfnc}{type}($vdirs->{$rdir}{cols}{$rfnc}{len})";
	}else{
	  $create .= "$col $vdirs->{$rdir}{cols}{$rfnc}{type}";
	}
      }else{
	if( defined $cols->{$col}{len} ) {
	  $create .= "$col $cols->{$col}{type}($cols->{$col}{len})";
	}else{
	  $create .= "$col $cols->{$col}{type}";
	}
	$create .= " $cols->{$col}{colopt}" if defined $cols->{$col}{colopt};
      }
      $create .= ",";
    }
    chop $create;
    $create .= ")";
    $r = $dbh->do( $create );
    unless( $r ) {
      print "$PRG: create table $tab:\n  $DBI::errstr\n";
      return;
    }
    my $df = $vdirs->{$tab}{defaultfile} if exists $vdirs->{$tab}{defaultfile};
    if( $df ) {
      my $fnc = $vdirs->{$tab}{fnamcol};
      if( (length($df)<=$vdirs->{$tab}{cols}{$fnc}{len}) and !($df=~/\W+/)) {
	$r = $dbh->do( "insert into $tab ($fnc) values ('$df')");
	if( !defined $r or $r==0 ) {
	  print "ERROR: couldn't create default entry '$df' in '/$tab'\n";
	}
      }else{
	print "ERROR: illegal or to long default filename '$df' in '/$tab'\n";
      }
    }
  }
  return;
}

########################################################################
# print_file( FH, fnam, verbose );
# create new pseudo file for cat/vi from database
#   FH:		file handle for output
#   fnam:	the filename (key from db)
#   verbose:	0: exclude comments, print only if fnam exists
#	 	1: include comments, print only if fnam exists
#	 	2: include comments, always print: print values if fnam exists,
#		   else print NULL values
# return:
# 	0: Ok
#	1: file does not exist, (but NULL valued file was printed if verbose=2)
#	2: other error
#
sub print_file() {
  my $FH = shift;
  my $fnam = shift;
  my $verbose = shift;

  my @vars;
  my @dbvars;
  my $var;
  my $maxvarlen = 0;
  my @values;
  my @defaults;
  my @descs;
  my @isref;
  my $select = "select ";
  my $retval = 2;

  # prepare db query
  my $fnc = $vdirs->{$vwd}{fnamcol};
  my $cols = $vdirs->{$vwd}{cols};
  foreach my $col (sort {%{$cols}->{$a}{pos} <=> %{$cols}->{$b}{pos}} 
		   keys(%{$cols}) ) 
    {
      next if $col eq $fnc;
      $var = $cols->{$col}{var};
      if( length($var) > $maxvarlen ) {$maxvarlen = length($var); }
      push @vars,  $var;
      push @dbvars,$col;
      push @descs, $cols->{$col}{desc};
      push @isref, defined $cols->{$col}{ref} ? $cols->{$col}{ref} : undef;
      $select .=  "$col,";
    }
  chop $select;
  $select .= " from $vwd where $fnc=?";
  
  # query db
  my $st;
  $st = $dbh->prepare( $select );
  unless( $st ) {
    print $FH "$PRG: can't prep print query '$vwd':\n  $DBI::errstr\n";
    return 2;
  }
  unless( $st->execute( $fnam ) ) {
    print $FH "$PRG: can't exec print query 1 '$vwd' :\n  $DBI::errstr\n";
    return 2;
  }
  @values = $st->fetchrow_array();
  $st->finish();

  if( $vdirs->{$vwd}{defaultfile} and $vdirs->{$vwd}{defaultfile} ne $fnam ) {
    unless( $st->execute( $vdirs->{$vwd}{defaultfile} ) ) {
      print $FH "$PRG: can't exec sum query 2 '$vwd':\n  $DBI::errstr\n";
      return 2;
    }
    @defaults = $st->fetchrow_array();
  }
  $st->finish();
  
  # print it
  my $em = "*unset*";

  if( $verbose == 0 ) {
    if( @values ) {
      # print short version (command 'sum')
      $retval = 0;
      for( my $i=0; $i<= $#values; $i++ ) {
	printf $FH "%-${maxvarlen}s ", $vars[$i];
	if( @defaults ) {
	  if( defined $values[$i] ) {
	    print $FH  " = $values[$i]\n";
	  }else{
	    print $FH defined $defaults[$i] ? "-> $defaults[$i]\n" :"-> $em\n";
	  }
	}else{
	  print $FH defined $values[$i] ? "= $values[$i]\n" : "= $em\n";
	}
      }
    }else{
      $retval = 1;
    }
  }else{
    # verbose == 1: (print long)   2: (print long, even if file does not exist)
    my $newfilemsg = "";
    my $print_it = 0;
    if( @values ) {
      # file exists
      $retval   = 0;
      $print_it = 1;
    }else{
      # file does not exist
      $retval = 1;
      if( $verbose == 2 ) {
	$newfilemsg = "#\n#  NEW FILE   NEW FILE   NEW FILE   NEW FILE\n#\n";
	$print_it = 1;
	for( my $i=0; $i<= $#vars; $i++ ) {
	  $values[$i] = undef;
	}
      }
    }
    if( $print_it == 1 ) {
      # command 'cat/vi': long version
      print $FH "$newfilemsg" ;
      print $FH "#\n# Settings for $vdirs->{$vwd}{cols}{$fnc}{var} '$fnam'" ;
      if( $vdirs->{$vwd}{defaultfile} and 
	  $vdirs->{$vwd}{defaultfile} ne $fnam ) {
	print $FH " (defaults: '$vdirs->{$vwd}{defaultfile}')";
      }
      print $FH "\n#\n".
	"# - this is a comment, comments always start in the first column.\n".
	"# - all lines begin in the first column or are blank lines\n".
	"# - a unset variable will write NULL into the database column\n";
      if( $vdirs->{$vwd}{defaultfile} and 
	  $vdirs->{$vwd}{defaultfile} ne $fnam ) {
	print $FH "# - unset variables use the default values\n";
      }
      print $FH "#\n";
      for( my $i=0; $i<= $#values; $i++ ) {
	# variable with comment header
	printf $FH "\n# %-50s(%s)\n", $vars[$i], $dbvars[$i];
	foreach my $descline (split '\n', $descs[$i] ) {
	  print $FH "# $descline\n";
	}
	print $FH "#\n";
	if( @defaults ) {
	  print $FH  "# default: ";
	  print $FH  defined $defaults[$i] ? "$defaults[$i]\n#\n" : "$em\n#\n";
	}
	print $FH &var_value( $vars[$i], $values[$i], $isref[$i] );
      }
      print $FH "\n# end of file '$fnam'\n";
    }
  }
  return $retval;
}

########################################################################
# var_value( var, value, ref )
# return a var = value string for verbose print_file()
#   var:	variable name (long version for cat/vi)
#   value:	the value of var or undef
#   ref:	the dir/table referenced by this var or undef
# return:
# 	the string to be printed
#
sub var_value() {
  my ($var, $value, $ref ) = @_;
  my $s = '';
  if( defined $ref ) {
    # query db
    my $rval;
    my $st;
    my $select = 
      "select $vdirs->{$ref}{fnamcol} from $ref order by $vdirs->{$ref}{fnamcol}";
    $s .= "#   This is a reference to a file in dir '$ref'.\n";
    $st = $dbh->prepare( $select );
    unless( $st ) {
      $s .= "$PRG: can't prep var query '$ref':\n  $DBI::errstr\n";
      return $s;
    }
    unless( $st->execute( ) ) {
      $s .= "$PRG: can't exec var query '$ref' :\n  $DBI::errstr\n";
      return $s;
    }
    $s .= "$var = \n" unless defined $value;
    my $found = 0;
    while( ($rval) = $st->fetchrow_array() ) {
      if( defined $value and $value eq $rval ) {
	$found = 1;
      }else{
	$s .= "#";
      }
      $s .= "$var = $rval\n";
    }
    $st->finish();
    if( $found == 0 and defined $value ) {
      $s .= "### NOTE: File '$value' does not exist in dir '$ref'!\n";
      $s .= "### NOTE: This value will be rejected when saving!\n";
      $s .= "$var = $value\n";
    }
  }else{
    $s .= "$var = ";
    $s .= "$value" if defined $value;
    $s .= "\n";
  }
  return $s;
}

########################################################################
# get_who_refs_me( dir, file )
# return all files referenced by FILE
#   dir:	an existing directory
#   file:	the (probably existing) file within DIR which references 
#		to be checked
# return:
# 	- a list of strings in format "dir/file" if references are found
#	- empty list if no references are found
#	- a list whith one entry holding the errormessage in case of an error
#
sub get_who_refs_me() {
  my ($dir,$file) = @_;
  my @res = ();

  foreach my $refdir (sort keys(%{$vdirs->{$dir}{refby}}) ) {
    my $select = "select $vdirs->{$refdir}{fnamcol} from $refdir where ";
    my @rcols = @{$vdirs->{$dir}{refby}{$refdir}};
    my $st;
    map { $_ .= "='$file'" } @rcols;
    $select .= join( " or ", @rcols );
    $select .= " order by $vdirs->{$refdir}{fnamcol}";

    $st = $dbh->prepare( $select );
    unless( $st ) {
      push @res,"$PRG: can't prep wrefs query '$file':\n  $DBI::errstr\n";
      return @res;
    }
    unless( $st->execute( ) ) {
      push @res,"$PRG: can't exec wrefs query '$file':\n  $DBI::errstr\n";
      return @res;
    }
    my $reffile;
    while( ($reffile) = $st->fetchrow_array() ) {
      push @res, "$refdir/$reffile";
    }
    $st->finish();
  }
  return @res;
}

########################################################################
# create_sql_from_file( tempfile, dir, vfile, insert_flag );
#
# tmpfile:	Absolute pathe to temporary file on local disk holding
#		the edited parameters
# vdir:		exisiting virtual dir (table)
# vfile:	A file (db-row) for which to generate the $sql SQL code
# insert_flag:	0: $sql --> 'update' string    1: $sql --> 'insert' string
#
# return
# 	a list: ($linenumber,$err, $sql):
#	- $linennumber: when an error was detected: the errornous line
# 	- $err: when an error was detected: a one line error text, else: undef 
#	        when $err is set then $sql is invalid
#	- $sql: when no error was detected: a SQL insert/update string or ''
#	      	   if nothing to do, when $err is set: trash or undef
#	
#
sub create_sql_from_file( ) {
  my ($tmpfile,$vdir,$vfile,$insert_flag) = @_;
  my $lineno = 0;
  my $line;
  my $var;
  my $val;
  my $err;
  my $sql1;
  my $sql2;
  my %varcol;			# translataion varname -> columnname
  my %isset;			# flags: variable already set? 1: yes
  my %filevars;			# variables from file for phase 2
  my %filevarslineno;		# lineno of variables from file for phase 2

  if( $insert_flag ) {
    $sql1 = "insert into $vdir ($vdirs->{$vdir}{fnamcol},";
    $sql2 = " values('$vfile',";
  }else{
    $sql1 = "update $vdir set ";
    $sql2 = " where $vdirs->{$vdir}{fnamcol}='$vfile'";
  }
  # setup varname translation
  my $cols = $vdirs->{$vdir}{cols};
  foreach my $col ( keys( %{$cols} ) ) {
    $varcol{ $cols->{$col}{var} } = $col;
  }

  # phase 1: do the basic checks, remember var values and their lineno for 
  #	     phase 2 check (user supplied check functions)
  open( TF, $tmpfile ) or return ( 1,"can't open tempfile '$tmpfile'", undef );
  while( <TF> ) {
    $line = $_;
    $lineno++;
    chop( $line );
    $line =~ s/^\s*//;		# remove leading space
    next if $line =~ /^$/;	# skip empty lines
    next if $line =~ /^\#.*/;	# skip comment lines
    unless( $line =~ /=/ ) {	# missing = ?
      $err = "line $lineno: missing '='";
      last;
    }
    ($var,$val) = split( /=/, $line, 2 );
    $var =~ s/\s*$//;		# remove trailing space
    $val =~ s/^\s*//;		# remove leading space
    $val =~ s/\s*$//;		# remove trailing space

    if( length($var)==0 or $var =~ /\W+/ ) {	# var name ok?
      $err = "line $lineno: syntax error";
      last;
    }

    # check if variable name exists
    if( defined $varcol{$var} ) {
      if( defined $isset{$var} ) {
	$err = "line $lineno: variable '$var' set twice"; 
	last;
      }

      my $col = $varcol{$var};
      my $vlen = length( $val );
      $val =~ s/\\/\\\\/g;	# protect db specific chars 
      $val =~ s/\'/\\\'/g;
      if(  $vlen > 0 ) {
	# check types
	if( defined $cols->{$col}{ref} ) {
	  # type ref
	  my $rdir = $cols->{$col}{ref};
	  my $rfnc = $vdirs->{$rdir}{fnamcol};
	  if( defined $vdirs->{$rdir}{cols}{$rfnc}{len} ) {
	    my $rlen = $vdirs->{$rdir}{cols}{$rfnc}{len};
	    if( $vlen > $rlen ) {
	      $err = "line $lineno: value longer than $rlen"; 
	      last;
	    }
	  }else{
	    if( $vlen > 1 ) {
	      $err = "line $lineno: value longer than 1"; 
	      last;
	    }
	  }
	  # check if val exists in referneced table
	  my $st;
	  my $dbval;
	  $st = $dbh->prepare("select $rfnc from $rdir where $rfnc=?");
	  unless( $st ) {
	    $err = "$PRG: internal error: prepare 'exist' query '$rdir':\n";
  	    $err .= "  $DBI::errstr\n";
	    last;
	  }
	  unless( $st->execute( $val ) ) {
	    $err = "$PRG: internal error: exec 'exist' query '$rdir':\n";
  	    $err .= "  $DBI::errstr\n";
	    last;
	  }
	  $dbval = $st->fetchrow_array();
	  $st->finish();
	  unless( defined $dbval ) {
	    $err = "line $lineno: reference '$val' does no exist in '$rdir'";
	    last;
	  }
	  if( $insert_flag ) {
	    $sql1 .= "$col,";
	    $sql2 .= "'$val',";
	  }else{
	    $sql1 .= "$col='$val',";
	  }
	  $filevars{$col}       = $val;
	  $filevarslineno{$col} = $lineno;

	}elsif( $cols->{$col}{type} eq 'char' ) {
	  # type char
	  if( defined $cols->{$col}{len} ) {
	    if( $vlen > $cols->{$col}{len} ) {
	      $err = "line $lineno: value longer than $cols->{$col}{len}";
	      last;
	    }
	  }else{
	    if( $vlen > 1 ) {
	      $err = "line $lineno: value longer than 1"; 
	      last;
	    }
	  }
	  if( $insert_flag ) {
	    $sql1 .= "$col,";
	    $sql2 .= "'$val',";
	  }else{
	    $sql1 .= "$col='$val',";
	  }
	  $filevars{$col}       = $val;
	  $filevarslineno{$col} = $lineno;

	}elsif( $cols->{$col}{type} eq 'int' ) {
	  # type int
	  unless( $val =~ /^-?\d+$/ ) {
	    $err = "line $lineno: value not an integer"; 
	    last;
	  }
	  if( $val <= -2147483648 or $val >= 2147483647 ) {
	    $err = "line $lineno: value out of int range"; 
	    last;
	  }
	  if( $insert_flag ) {
	    $sql1 .= "$col,";
	    $sql2 .= "$val,";
	  }else{
	    $sql1 .= "$col=$val,";
	  }
	  $filevars{$col}       = $val;
	  $filevarslineno{$col} = $lineno;

	}elsif( $cols->{$col}{type} eq 'smallint' ) {
	  # type smallint
	  unless( $val =~ /^-?\d+$/ ) {
	    $err = "line $lineno: value not an integer"; 
	    last;
	  }
	  if( $val <= -32768 or $val >= 32767 ) {
	    $err = "line $lineno: value out of smallint range"; 
	    last;
	  }
	  if( $insert_flag ) {
	    $sql1 .= "$col,";
	    $sql2 .= "$val,";
	  }else{
	    $sql1 .= "$col=$val,";
	  }
	  $filevars{$col}       = $val;
	  $filevarslineno{$col} = $lineno;

	}else{
	  # type unknown!
	  $err = "line $lineno: unsupported datatype from vdirs for $var"; 
	  last;
	}
      }else{
	if( $insert_flag ) {
	  $sql1 .= "$col,";
	  $sql2 .= "NULL,";
	}else{
	  $sql1 .= "$col=NULL,";
	}
	$filevars{$col}       = undef;
	$filevarslineno{$col} = $lineno;
      }
      $isset{$var} = 1;	# remember that this var is set
    }else{
      $err = "line $lineno: unknown variable '$var'";
      last;
    }
  }
  close( TF );
  if( $insert_flag ) {
    chop( $sql1 );
    chop( $sql2 );
    $sql1 .= ")";
    $sql2 .= ")";
  }else{
    if( chop( $sql1 ) ne ',' ) {
      # no columns to update
      $sql1 = "";
      $sql2 = "";
    }
  }

  # phase 2: if basic check didn't show an error, do the user supplied checks

  $filevars{ $vdirs->{$vdir}{fnamcol} } = $vfile;  # add our filename to hash
  if( !defined $err ) {
    foreach my $col (keys(%filevars) ) {
      my $valerr;
      if( exists $cols->{$col}{valok} ) {
	$valerr = &{$cols->{$col}{valok}}( $filevars{$col}, \%filevars, $dbh );
	if( defined $valerr ) {
	  $err = "line $filevarslineno{$col}: $valerr";
	  $lineno = $filevarslineno{$col};
	  last;
	}
      }
    }
  }
  return ( $lineno, $err, "$sql1$sql2" );
}

########################################################################
# want_to_edit_again( errortext )
# ask the user if he wants to edit again
#   errortext:	one line error text
# return:
# 	'y' or 'n'
#
sub want_to_edit_again() {
  my $errortext = shift;
  my $inp = '';
  my $IN = $term->IN;
  print $OUT "\n\n\n\n\n\n\nERROR: $errortext\n";
  while( $inp ne 'y' and $inp ne 'n' ) {
    print $OUT "Do you want to edit again ('n' will abort) [y/n] ? ";
    $inp = <$IN>; 
    $inp = '\n' unless defined $inp;
    chop $inp;
  }
  return $inp;
}

########################################################################
########################################################################
########################################################################
########################################################################

1;
__END__


=head1 NAME

DBIx::FileSystem - Manage tables like a filesystem


=head1 SYNOPSIS

  use DBIx::FileSystem;
  my %vdirs = 
  ( 
     table_one => 
     { 
       # ... column description here ...
     },
     table_two => 
     { 
       # ... column description here ...
     },
  );

  my %customcmds = ();

  if( $#ARGV==0 and $ARGV[0] eq 'recreatedb' ) {
    recreatedb(%vdirs, $PROGNAME, $VERSION, 
	       $DBHOST, $DBUSER, $DBPWD);
  }else{
    # start the command line shell
    mainloop(%vdirs, $PROGNAME, $VERSION, 
	     $DBHOST, $DBUSER, $DBPWD, %customcmds );
  }

This synopsis shows the program (aka 'the shell') to manage the 
database tables given in hash B<%vdirs>.

=head1 DESCRIPTION

The module DBIx::FileSystem offers you a filesystem like view to
database tables. To interact with the database tables, FileSystem
implements a command line shell which offers not only a subset of well
known shell commands to navigate, view and manipulate data in tables, but
also gives the convenience of history, command line editing and tab
completion. FileSystem sees the database as a filesystem: each
table is a different directory with the tablename as the directory
name and each row in a table is a file within that directory. 

The motivation for FileSystem was the need for a terminal based
configuration interface to manipulate database entries which are used
as configuration data by a server process. FileSystem is neither
complete nor a replacement for dbish or other full-feature SQL shells
or editors. Think of FileSystem as a replacement for a Web/CGI based
graphical user interface for manipulating database contents.


=head1 REQUIREMENTS

The DBI module for database connections.  A DBD module used by DBI for
a database system.  And, recommended, Term::ReadLine::Gnu, to make
command line editing more comfortable, because perl offers only stub
function calls for Term::ReadLine. Note: Term::ReadLine::Gnu requires
the Gnu readline library installed.

=head1 FUNCTIONS



recreatedb(%vdirs,$PROGNAME,$VERSION,$DBHOST,$DBUSER,$DBPWD);


Recreate the tables given in B<%vdirs>. Will destroy any existing tables
with the same name in the database including their contents. Will create
a table 'tablestatus' for version information.
Tables not mentioned in B<%vdirs> will not be altered. 
The database itself will 
not be dropped. Checks if B<%vdirs> is valid. Returns nothing.


mainloop(%vdirs,$PROGNAME,$VERSION,$DBHOST,$DBUSER,$DBPWD,%customcmds);

Start the interactive shell for the directory structure given in B<%vdirs>. 
Returns when the user quits the shell. Checks if B<%vdirs> is valid. 

=head2 parameters

=over 4

=item %vdirs

A hash of hashes describing the database layout which will be under 
control of FileSystem. See DATABASE LAYOUT below for details.

=item $PROGNAME

The symbolic name of the interactive shell. Used for errormessages and command
prompt.

=item $VERSION

A character string with max. length of 16. Holds the version number of
the database layout. See VERSION INFORMATION below for details.

=item $DBHOST

DBI connect string to an existing database. Depends on the underlying 
database. Example: "dbi:Pg:dbname=myconfig;host=the_host";

=item $DBUSER

DBI database user needed to connect to the database given in $DBHOST.

=item $DBPWD

DBI password needed by $DBUSER to connect to the database given in $DBHOST.
May be set to undef if no password checking is done.

=item %customcmds

A hash which contains user defined commands to extend the shell 
(custom commands).
If you do not have any commands then set %customcmds = (); before calling
mainloop(). The key of the hash is the commandname for the shell, the value 
is an anon hash with two fields: B<func> holding a function reference of the
function implementing the command and B<doc>, a one line help text for the
help command. 

=back

=head2 custom commands

All custom commands are integrated into the completion functions: command 
completion and parameter completion, where parameter completion uses the
files in the current directory.

A custom command gets the shells command line parameters as 
calling parameters. DBIx::FileSystem exports the following variables for use
by custom commands:

=over 4

=item $DBIx::FileSystem::vwd

The current working directory of the shell. Do not modify!

=item $DBIx::FileSystem::dbh

The handle to the open database connection for the config data. Do not modify!

=item $DBIx::FileSystem::OUT

A fileglob for stdout. Because FileSystem / Gnu ReadLine grabs the tty stdout
you can not directly print to stdout, instead you have to use this fileglob.
Do not modify!

=back

Please see 'count' command in the example 'pawactl' how to implement custom
commands. See the source of DBIx::FileSystem how to implement commands.

=head1 TRANSACTIONS

FileSystem uses autocommit when talking with the database. All operations
done by FileSystem consist of one single SQL command.

=head1 DATABASE LAYOUT

FileSystem sees a table as a directory which contains zero or more
files. Each row in the table is a file, where the filename is defined
by a column configured with the B<%vdirs> hash. Each file holds some
variable = value pairs. All files in a directory are of the same
structure given by the table layout. A variable is an alias for a
column name, the value of the variable is the contents of the
database.

When editing a file FileSystem generates a temporary configuration
file with comments for each variable and descriptive variable names
instead of column names. The variable names and comments are defined
in B<%vdirs> hash as shown below. So, in the following description:

    'directory' is a synonym for 'table'
    'file'      is a synonym for 'row',
    'variable'  is a synonym for 'column'

=head2 DEFAULTFILE FUNCTION

Each directory optionally supports a defaultfile. The idea: If a
variable in a file has value NULL then the value of the defaultfile
will be used instead. The application using the database (for reading
configuration data from it) has to take care of a defaultfile.

FileSystem knows about a defaultfile when viewing a file and shows the 
values from the defaultfile when a variable contains NULL. A defaultfile
can not be removed with 'rm'.


=head2 B<%vdirs hash>

The B<%vdirs> hash defines the database layout. It is a hash of hashes.

 %vdirs = (
   DIRECTORY_SETTING,
   DIRECTORY_SETTING,
   # ... more directory settings ...
 );

The DIRECTORY_SETTING defines the layout of a directory (database table):

  # mandatory: the directory name itself
  dirname  => {			

    # mandatory: description of table
    desc => "dir description",	

    # mandatory: Defines if this directory is read-
    # only or writable for the shell. If set to 1
    # then the commands vi/rm are allowed.
    edit => [0|1],		

    # mandatory: The column which acts as filename.
    # The column must be of type 'char' and len 
    # must be set and len should be < 15 for proper
    # 'ls' output
    fnamcol => 'colname',	

    # optional: The column which acts as comment
    # field. The column must be of type 'char' and 
    # len must be set. The comments will be shown
    # by 'll' command (list long).
    comcol => 'colname',	

    # optional: Name of a default file. This file
    # will be automatically created from 
    # &recreatedb() and cannot be removed. The
    # defaultfile is only usefull when edit = 1.
    defaultfile => 'filename',	

    # optional: Function reference to a function 
    # that will be called when a file of this 
    # directory will be removed. The rm command
    # will call this function with parameters 
    # ($dir, $file, $dbh) of the file to be removed 
    # and after all other builtin checks are done.
    # $dbh is the handle to the database connection.
    # The function has to return undef if remove 
    # is ok, or a one line error message if it is 
    # not ok  to remove the file
    rmcheck => \&myRmCheckFunction,

    # mandatory: column settings
    cols => {		
  	COLUMN_SETTING,
  	COLUMN_SETTING,
	# ... more column settings ...
    },
  },

The COLUMN_SETTING defines the layout of a column (database column). 

  # mandatory: the columnname itself
  colname => {			

    # mandatory: column type of this column 
    # (see below COLUMN_TYPE)
    COLUMN_TYPE,

    # optional: extra constraints for this column
    # when creating the database with 
    # &recreatedb(). Example: 'NOT NULL'
    colopt => 'OPTIONS',		

    # optional: Function reference to a function
    # that will be called before a value gets
    # inserted/updated for this column and after
    # builtin type, length, range and reference
    # checks has been done. Will be called with
    # ($value_to_check,$hashref_to_othervalues,$dbh)
    # hashref holds all values read in from file,
    # key is the columnname. All hashvalues are 
    # already checked against their basic type, 
    # empty values in the file will be set to undef.
    # $dbh is the handle to the database connection.
    # The valok function has to return undef
    # if the value is ok or a one line error
    # message if the value is not ok.
    valok => \&myValCheck,			

    # optional: When this option exists and is set
    # to 1 then this column will be set to NULL 
    # when copying af file with 'cp'.
    delcp => 1,			

    # mandatory: Descriptive long variable name 
    # for this column. Will be used as an alias
    # for the columname for display or edit/vi.
    var => 'VarName',		

    # mandatory: One line description what this
    # variable is good for. Will be show up as
    # a comment when displaying (cat) or editing
    # (vi) this file.
    desc => "...text...",	

    # mandatory: A counter used to sort the columns
    # for display/editing. Smaller numbers come 
    # first. See example pawactl how to setup.
    pos => NUMBER,		
 },


The COLUMN_TYPE defines, if the column is a normal database column or
a reference to another file in another directory. A column is either a
normal column or a ref-column.

normal column:

    # mandatory: database type for this column. 
    # Allowed types:
    # - when this column acts as the filename
    #   ('fnamcol'in DIRECTORY_SETTING): char
    # - when edit=1 set in DIRECTORY_SETTING:
    #   char, int, smallint
    # - when edit=0 set in DIRECTORY_SETTING:
    #   char, int, smallint, date, bool, ...
    type => 'dbtype',		

    # optional: length of column. Only usefull 
    # when type => 'char'. Mandatory if this
    # column is used as the filename.
    len => NUMBER,		

   
ref-column:
   
    # mandatory: A directory name of another 
    # directory. Allowed values for this variable
    # will be existing filenames from directory 
    # 'dirname' or NULL. rm uses this information
    # to check for references before removing a 
    # file. editing/vi uses this information to
    # check a saved file for valid values.
    ref   => 'dirname',		


=head1 DATABASE CONSTRAINTS

The user can set database constraints for scpecific columns with the
B<colopt> option in B<%vdirs>. FileSystem takes care of these constraints
and reports any errors regarding the use of these constraints to the 
user. Because the errormessages (from the DBI/DBD subsystem) are sometimes
not what the user expects it is a good idea to use the custom 
B<rmcheck> and B<valok> functions within B<%vdirs> together with database 
constraints. This has more advantages:

=over 4

=item 1.

When using database constraints the database takes care about integrity. 
Other programs than FileSystem
can not destroy the integrity of the database.

=item 2.

FileSystem, B<rmcheck> and B<valok> custom functions report 'understandable' 
error messages to the user, they also report the errornous line number to the
editor after editing and saving an odd file. Database errors have no line 
numbers.

=item 3.

FileSystem functions, B<rmcheck> and B<valok> custom functions will be
called just before a database operation. If they fail, the database operation
will not take place.

=item 4.

FileSystem may be buggy.

=back


=head1 VERSION INFORMATION

When using FileSystem for managing configuration data for a server
process, you have three versions of database layout in use:

=over 4

=item 1.

database layout given in B<%vdirs> hash

=item 2.

database layout in the database itself

=item 3.

database layout the server process expects

=back

To make sure that all three participants use the same database layout
FileSystem supports a simple version number control. Besides the
tables given in B<%vdirs> FileSystem also creates a table called
'tablestatus'. This table has two columns, B<tag> and B<value>, both
of type char(16). FileSystem inserts one entry 'version' when
recreating the database and inserts the version string given as
parameter to &recreatedb. 

Before doing any operations on the database when calling &mainloop(), 
FileSystem
first checks if the version string given as parameter to &mainloop()
matches the version string from database in table 'tablestatus', row
'version', column 'value'. If they do not match, FileSystem terminates with
an error messages.

When modifying the B<%vdirs> hash it is strongly recommended to
change/increment the version number given to &mainloop() also.  To be
on the safe side you should recreate the database after changing
B<%vdirs>.  Keep in mind that you will loose all data in the tables
when calling &recreatedb(). Alternative way: Modify B<%vdirs> and
increment the version string for the &mainloop() call. Then start your
favourite SQL editor and manually change the database layout according
to B<%vdirs>.

The server
process should take care of the version number in 'tablestatus' also.

=head1 COMMAND SHELL

The command line shell offers command line history, tab completion and
commandline editing by using the functionality by using the installed
readline library. See the manpage B<readline(3)> for details and key bindings.

Supported commands are:

=over 5

=item cat

Usage: 'cat FILE'.
Show a file contents including generated comments. 

=item cd

Usage: 'cd DIR'. Change to directory DIR. The directory hierarchy is flat, 
there is no root
directory. The user can change to any directory any time. You can only
change to directories mentioned in the B<%vdirs> structure. FileSystem
does not analyze the system catalog of the underlying database.

=item cp

Usage: 'cp OLD NEW'. Copy file OLD to file NEW (clone a file). When
copying, the variables marked as 'delcp' will be set to NULL in file
NEW. Requires write access to the directory.

=item help

Usage: 'help [command]'. Show a brief command description.

=item ls

Usage: 'ls'. Show the contents of the current directory. The B<%vdirs>
hash defines, which columns are used as a filename.

=item ld

Usage: 'ld'. Show the contents (dirs and files) of the current directory 
in long format. The B<%vdirs> hash defines, which columns are used as a 
filename. For directories 'ld' will display the directory B<desc> field 
from B<%vdirs>. For files see command 'll' below.

=item ll

Usage: 'll'. Show the contents (files only) of the current directory, 
in long format. The B<%vdirs> hash defines, which columns are used as a 
filename. If B<comcol> (comment column) is set in B<%vdirs>, then 
additionally show the contents of this column for each file. 


=item quit

Usage: 'quit'. Just quit.

=item rm 

Usage: 'rm FILE'. Remove FILE. You can only remove files that are not
referenced. Reference checks are done by FileSystem using the
reference hierarchy given in the B<%vdirs> hash. To un-reference a file
set the reference entry in the referring file to NULL, to another file or
remove the referring file. 'rm' requires write access to the directory.

=item sum

Usage: 'sum FILE'. Show the summary of FILE. The summary only shows
the variables and their values, without any comments. 'sum' knows
about the 'defaultfile': If a FILE has variables = NULL and a
defaultfile is given, then sum shows '->' and the value of the
defaultfile instead of '=' and the value of the variable.

=item ver

Usage: 'ver'. Show version information.


=item vi

Usage: 'vi FILE'. Edit FILE with an editor. Starts the default editor
given in the shell environment variable $EDITOR. If this is not
defined, it starts C</usr/bin/vi>. After quitting the editor the file
will be checked for proper format and values. If there is something
wrong, the user will be asked to reedit the file or to throwaway the
file. 

In case of reediting a file because
saving was rejected, the editor is started over with '+LINENO' as the
first parameter to let the cursor directly jump to the error line. If
the editor given in $EDITOR does not support this syntax an error will occur.

If FILE does not exist it will be created after saving and quitting the
editor. This is usefull when a column has a 'NOT NULL' constraint.

Note: Only the values will be saved in the database. All comments made
in the file will get lost. If you need comments, add a 'comment'
Variable for this directory in B<%vdirs>.

Note: The file parser currently is very simple. Currently it is not
possible to assign a string of spaces to a variable.

=item wrefs

Usage: 'wrefs FILE'. Show who references FILE. Reference checks are
done by FileSystem using the reference hierarchy given in the B<%vdirs>
hash. Other references to FILE will not be detected because FileSystem
does not read the system catalog of the database. Note: A non-existing
FILE will not be referenced by anyone.

=back


=head1 BUGS

=over 4

=item -

M:N relations currently not supported.

=item -

composite primary keys currently not supported

=back

=head1 AUTHOR

Alexander Haderer	alexander.haderer@charite.de

=head1 SEE ALSO

perl(1), DBI(3), dbish(1), readline(3)

=cut
