#!/usr/bin/perl -w
#
# 2008-2009 Jeremie Le Hen <jeremie.le-hen@sgcib.com>
# 2004 Vincent Haverlant <vincent.haverlant@sgcib.com> 
# $Id: multremsh.pl 73 2007-11-12 13:12:23Z vhaverla $

use strict;
use Getopt::Long;
use Pod::Usage;
use File::Basename;
use POSIX qw(strftime mkdir);
use IO::Pipe;

my $dirname;
BEGIN {
	$dirname = $0;
	$dirname =~ s{\/?[^/]+$}{};
	if ($dirname eq '') { $dirname = '.' }
}
use lib $dirname;
use Job::Parallel;
use Job::Timed;

# Initialisation and default values
my $VERSION='$Id: multremsh.pl 73 2007-11-12 13:12:23Z vhaverla $';
my $logger_pri='user.err';
my $listemachine; 
my $scriptfile;
my $scriptoptions;
my @hostlist;
my $hostlistref=\@hostlist;
my @putfiles;
my $man;
my $ping=1;
my $semaphore_nb = 1; 
my $logdir;
my $onsuccess;
my $onerror;
my $sqlquery;
my $dontexec;
my $printhostname;
my $rsh='ssh';
my %rcp_commands = ( 'rsh' => 'rcp',
			 'remsh' => 'rcp',
			 'ssh' => 'scp');
my $ssh_opts = '-o StrictHostKeyChecking=no -o PasswordAuthentication=no -o NumberOfPasswordPrompts=0 -q';
my $logmessage;
my $loghandle;
my $noce=0;
my $thread_max=50;
my $runlocally;
my $nostamp;
my $furtive;
my ($_o_timeout, $timeout);
my $ssh_user = $ENV{'LOGNAME'};
my $ssh_keyfile;

# autoflush
$|=1;

# Command line options parsing
Getopt::Long::Configure ('gnu_getopt');
GetOptions(
	   'target|t=s' => \@hostlist,
	   'liste|l=s' => \$listemachine,
	   'script|s=s' => \$scriptfile,
		 'scriptoptions=s' => \$scriptoptions,
	   'message|m=s' => \$logmessage,
	   'put=s' => \@putfiles,
	   'logdir=s' => \$logdir,
	   'help|man|h' => \$man,
	   'L' => \$runlocally,
	   'N'=> \$dontexec,
	   'M'=> \$printhostname,
	   'ping!' => \$ping,
	   'rsh|e=s' => \$rsh,
	   'onsuccess=s' => \$onsuccess,
	   'onerror=s' => \$onerror,
	   'spawn=i' => \$semaphore_nb,
	   'sqllist=s' => \$sqlquery,
	   'ce=i' => \$noce,
	   'furtive|f' => \$furtive,
	   'timeout=i' => \$_o_timeout,
	   'user=s' => \$ssh_user,
	   'keyfile=s' => \$ssh_keyfile,
	   ) or (die $!);

if ($man) { 
	pod2usage(-verbose=>2) ;
	exit(0);
}

if (($_o_timeout) && ($_o_timeout > 119)) {
	$timeout=$_o_timeout;
}
else {
	$timeout=120;
}

if ($runlocally and @putfiles) {
	die "No need to push files when run locally";
}

################################################
# Checkl for SQL query
#if ($sqlquery && $sqlquery =~ /select name/i) {
#	my $dsn='DBI:mysql:database=parc;host=marley';
#	my $dbh=DBI->connect($dsn,'guest','');
#	my $sth=$dbh->prepare($sqlquery);
#	$sth->execute;
#	my $numRows=$sth->rows;
#	while (my $row = $sth->fetchrow_hashref) {
#	&printlog($row->{'Name'});
#	push @hostlist, $row->{'Name'};
#	}
#}
###############################################

if ((!$listemachine && !@hostlist) || !$scriptfile || !$logmessage) {
	print "Some things misssing\n";
	pod2usage(1);
	exit(0);
}

my $rcp = $rcp_commands{$rsh};
if ($rsh eq 'ssh') {
	if($ssh_keyfile) {$ssh_opts .= ' -i '.$ssh_keyfile;}
	$rsh = $rsh . ' ' . $ssh_opts;
	$rcp = $rcp . ' ' . $ssh_opts;
}

###############################################
# Check log directory or create it
###############################################

if ($logdir && !-d $logdir) {
	mkdir $logdir,0700;
}

###############################################
# Parse machine list file
# remove black lines and comments (#)
###############################################

if ($listemachine) {
	open LISTE, "<$listemachine" or die "Could not open machine list file $listemachine for reading\n";
	while (<LISTE>) {
	s/#.*$//;
	chomp;
	s/\s+//g;
	if (/\S+/){
		push @hostlist, $_;
	}
	}
}

exit if defined($dontexec);

## args checks
if ($scriptfile) {
	$scriptfile =~ /\.\./ and die "script file name contains illegal '..'";
	#$scriptfile =~ / / and die "script file name contains illegal ' '";
}
if(@putfiles) {
	grep (/\.\./, @putfiles) and die "Argument putfiles contains illegal '..'";
	grep (/\s+/, @putfiles) and die "Argument putfiles contains illegal ' '";
	
}
$semaphore_nb=($semaphore_nb>$thread_max?50:$semaphore_nb);
my $scriptname;
if (not $runlocally) {
  $scriptname = basename($scriptfile).rand(100000);
} else {
  $scriptname = $scriptfile;
}
&printlog('',"== Thread number=$semaphore_nb");


#############################
# Sub routine to print logs
#############################
sub printlog {
	my ($tag, @text) = @_;
	my $now_string = strftime("[%a %b %e %H:%M:%S %Y]", localtime);
	print $now_string," ($tag) ",@text,"\n";
	if (defined $loghandle) { print $loghandle @text,"\n" }
}

#############################
# Routine d'execution
#############################

#
# This function creates a pipe, forks and sets it as stdout before running the
# child.  The parent process reads the pipe and writes it to the log.
# The return value is the exit status of the executed command.
sub pipedrun {
	my ($timer, $command, $slaveid) = @_;

	# It would be easier to use open()'s pipe feature, but we wouldn't
	# be able to get the return status of the command.
	my $pipe = new IO::Pipe;
	my $pid = fork;
	if (not defined $pid) {
		&printlog($slaveid, "== Can't execute command: (fork) $!");
		return 300;
	}
	if ($pid == 0) {
		$pipe->writer();
		# Set pipe as stdout.
		my $stdout = \*STDOUT;
		if (not defined $stdout->fdopen($pipe->fileno, 'w')) {
			&printlog($slaveid, "== Can't execute command: (fdopen) $!");
			exit 127;
		}
		# Shutdown a warning from Perl: it yells when something else than
		# "exit" is called after "exec".
		no warnings;
		exec $command;
		&printlog($slaveid, "== Can't execute command: (exec) $!");
		exit 127;
	}
	$pipe->reader();
	while (<$pipe>) {
		chomp;
		&printlog($slaveid, $_);
	}
	$pipe->close;
	waitpid $pid, 0;
	my $status = $?;
	if ($status & 127) {
		&printlog($slaveid, "== Command killed with signal ".($status & 127));
		return 301;
	}
	return ($status >> 8);
}


sub timedrun {
	my ($timer,$command,$slaveid)=@_;

	&printlog($slaveid, "== Running $timer $command");
	my $status = &Job::Timed::runSubr($timer, \&pipedrun, @_);
	if (not defined $status) {
	&printlog($slaveid, "== Job::Timed::runSubr: ".&Job::Timed::error());
	return 400;
	}
	return $status;
}

#############################
# Print BEGIN/END message on remote host
# - Argument 0/1 means begin/end
#############################
sub stamp_ce {
	my ($host, $stamp, $slaveid)=@_;
	return 0 if ($noce==-1);
	return 0 if ($furtive);
	my $retval=&timedrun(30,"$rsh ".$ssh_user."@".$host." 'logger -p $logger_pri \"$noce: $logmessage $stamp\"'",$slaveid);
	return $retval;
}


###############################################
# Threaded routine
# Check host availability
# send script to the host, then execute it.
###############################################

sub traite {
	my ($slaveid, $jobid, $host, $jobmax) = @_;
	my $status;

	$SIG{'INT'} = $SIG{'TERM'} = \&Job::Timed::terminate;

	if ($logdir) {
	if (not defined open $loghandle, ">>$logdir/$host") {
		&printlog($slaveid, "== WARNING: Cannot open '$logdir/$host' for writing: $!");
	} else {
		# Shared among multiple process, so disable buffering.
		$loghandle->autoflush;
	}
	}

	&printlog($slaveid, "============= $host ($jobid/$jobmax)");
	if ($ping) {
	if (&timedrun(5, "ping -c 1 $host >/dev/null 2>&1", $slaveid)) {
		&printlog($slaveid, "== ERROR: Cannot ping '$host'");
		$status = 500;
		goto OUT;
	}
	}

	if ($runlocally) {
	$status = &timedrun($timeout, "chmod +x $scriptname && $scriptname $scriptoptions $host", $slaveid);
	if ($status) {
			&printlog($slaveid, "== ERROR: Script returned a non-zero status $status");
	}
	goto OUT;
	}

	$status = &stamp_ce($host, "BEGIN", $slaveid);
	if ($status) {
	&printlog($slaveid, "== ERROR: Can't connect to host '$host'");
	$status = 505;
	goto OUT;
	}

	if (@putfiles) {
	&printlog($slaveid, "== Uploading ",join(' ', @putfiles)," to $host:/tmp/");

	foreach my $putfile (@putfiles) {
		$status = &timedrun(60, "$rcp -r $putfile ".$ssh_user."@".$host.":/tmp/",$slaveid);
		if ($status) {
			&printlog($slaveid, "== ERROR: Failed to push '$putfile' on host '$host'");
		}
		goto OUT;
	}
	}

	&printlog($slaveid, "== Uploading $scriptfile to $host:/tmp/");
	$status = &timedrun(60,"$rcp $scriptfile ".$ssh_user."@".$host.":/tmp/$scriptname",$slaveid);
	if ($status) {
	&printlog($slaveid, "== ERROR: Failed to push '$scriptfile' on host '$host'");
	goto OUT;
	}

	$status = &timedrun($timeout,"$rsh ".$ssh_user."@".$host." 'cd /tmp && chmod +x $scriptname && ./$scriptname 2>&1 && echo 0'",$slaveid);

	&stamp_ce($host, "END", $slaveid);
	&timedrun(60,"$rsh ".$ssh_user."@".$host." 'cd /tmp && rm -f $scriptname'",$slaveid);

	if ($status) {
	&printlog($slaveid, "== ERROR: Script returned a non-zero status $status on host '$host'");
	}

OUT:
	&printlog($slaveid, "== End of thread");
	return $status;
}


############################################################
#			  Actually do the work:
# loop over machine list and start as many threads as needed
# threads are detached to be sure to free resources...
############################################################

$SIG{'INT'} = $SIG{'TERM'} = sub {
	Job::Parallel::terminate();
	if (!Job::Parallel::isChild()) { exit 0 }
};

Job::Parallel::run($semaphore_nb, \&traite, [ scalar @hostlist ], @hostlist);


#############################################################
# _______________
#< Documentation >
# ---------------
#   \
#	\
#		.--.
#	   |o_o |
#	   |:_/ |
#	  //   \ \
#	 (|	 | )
#	/'\_   _/`\
#	\___)=(___/
#############################################################


__END__

=head1 NAME

	mulremsh.pl - Run a given script on many hosts at the same time.

=head1 SYNOPSIS

	./multremsh.pl [options]

Options:
	-t|--target=<hostname>		   target hostname, you can use this option multiple time for multiple targets
	-l|--liste=<filename>					
	-s|--script=<filename>
		--scriptoptions=<options>
	-m|--message=<syslog message>
	--ce=<CE number>
	--put=<filename|dirname>
	--logdir=<log directory>
	-e|--rsh=<rsh|ssh>
	--spawn=<nthreads>
	-h|--help|--man
	-L
	-N 
	-M
	--timeout=<timeout>					time to wait for command execution in seconds
	--user=<ssh username>		   Username used to make ssh connections
	--keyfile=<ssh private key file>		Private key used to make ssh connections

Exemples:
	./multremsh.pl --spawn=10 -e ssh --logdir=logs/ -m "REL 162663: install jrockit 1.4.2_13" -t gigaprod05 \
				   -t gigaprod06 -t pnlgui001 -t pnlgui002 -s scripts/Linux.jrockit-jdk1.4.2_12.sh \
				   --put=scripts/Linux.jrockit-jdk1.4.2_12.tar.gz

=head1 OPTIONS

=over
=item B<-t> I<hostname>, B<--taget>=I<hostname>

	Target hostname. You can repeat this options to build a list of hosts. 
E.g.: --target=hosta -t hostb

=item B<-l> I<filename>, B<--liste>=<filename>

	name of a file containing a list of hostnames. Accepts one host per line. 
# is considered a comment and blank lines are ignored.

=item B<-s> I<filename>, B<--script>=<filename>
	
	Name of the file which will be sent on target:/tmp and executed. It can be 
a shell script, or a binary executable.

=item B<--scriptoptions>=<options>

		Options to give to the called script, must be formatted with double-quotes (ex. : "-c config.ini").
		Will be empty by default.

=item B<-m> I<syslog message>, B<--message>=I<syslog message>
	
	Type in a message that will be issued on the target via logger using 
user.errfacility/level. It is strongly recommended to put here a CE or WR 
number.
	!!! This field is mandatory !!!

=item B<--ce>=I<Calendar Event>
	
	Serial number of the planned intervention. Put here a Calendar Event or Work
Request number. It will be prepended in the logged message on the target syslog.

=item B<--put>=I<filename>

	Files to be put in /tmp on destination host. Transfer is done using either
rcp -r or scp -r so it works for directories. Any trainling / is stipped from
the argument to avoid undocumented copy troubles in case of directories. You can
use several --put options to build a list of files/directories.

=item B<--logdir>=I<log directory>

	Specify a log directory in which to put target logs. One file per target.
Log Filename is the hostname of the target.

=item B<-h>, B<--help>, B<--man>

	Prints this help

=item B<-L>

	Run the script locally with a server (-t) or a list of server (-l) in
argument.

=item B<-N>

	Simulates action but does not send files nor executes remote commands

=item B<-M>

	Prepends the target hostname in the log messages


=head1 POWER USER OPTIONS !!! USE WITH CARE !!!

=item B<-e> I<[rsh|ssh|remsh]>, B<--rsh>=I<[rsh|ssh|remsh]>
	
	Uses the specified remode command type

=item B<--spawn>=I<number>

	Number of parallel threads to spawn. !!! USE WITH CARE !!! There is a risk
of resource starvation due to the way rsh works. In any case it is not
considered safe to spawn more than 15 simultaneous threads.

=item B<--timeout>=I<timeout>
	
	Use this option to override default timeout value in seconds to wait for good 
command execution. Minimum value is 120 secs.  This option applies only to the script 
execution time. All other actions (preparations, file copies etc...) retain their 
default timers of 60s.

=item B<--user>=I<ssh user>

	Use this option to specify a username to make the ssh connections. 
Default for this option is current username of the sessions ($LOGNAME)

=item B<--keyfile>=I<ssh private key file>

	Use this option to specify a private key file to use to make the ssh 
connections. Default is to use the sonfigured values of the ssh tool for the ssh user.

=head1 NOTES
	
	Here is a little recommandation on the use of this script. It is preferable
to use the put option to put files on the remote host. It does not preserve file
permissions so you should first perform a chmod +x on any script that you intend
to call from the main script.

	Take care that the main script really exits. It is often a good idea to use
nohup subscript.sh & if there is a risk that you script will spawn processes
that you do not know of and thus will block the rsh. This can happen on linux
when restarting init.d services: /etc/init.d/ftpd restart for example.

	A kind of timedrun function will be implemented in the future to prevent the
above issue to happen.

=head1 AUTHOR

	Vincent Haverlant <vincent.haverlant@sgcib.com>

=head1 COPYRIGHT AND LICENSE

	This program is free software; you may redistribute it and/or modify
	it under the same terms as Perl itself.

=cut

