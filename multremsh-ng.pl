#!/usr/bin/perl -w
#
# 2008-2009 Jeremie Le Hen <jeremie.le-hen@sgcib.com>
#
# Based on multremsh.pl:
# 2004 Vincent Haverlant <vincent.haverlant@sgcib.com> 
# $Id: multremsh.pl 73 2007-11-12 13:12:23Z vhaverla $

use strict;
use Getopt::Long;
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
my @commands;
my @hosts;
my @putfiles;
my $ping=0;
my $semaphore_nb = 1; 
my $verbose = 0;
my $logdir;
my $rsh='ssh';
my %rcp_commands = ( 'rsh' => 'rcp',
			 'remsh' => 'rcp',
			 'ssh' => 'scp');
my $ssh_opts = '-o StrictHostKeyChecking=no -o PasswordAuthentication=no -o NumberOfPasswordPrompts=0 -q';
my $syslogmsg;
my $loghandle;
my $thread_max=50;
my $runlocally;
my ($_o_timeout, $timeout);
my $ssh_user = $ENV{'LOGNAME'};
my $ssh_keyfile;
my $sep = "on";

# autoflush
$|=1;

my $os;
my $pingcmd;

$os = `uname -s`;
chomp $os;
$pingcmd = 'ping -c 1';
if ($os eq 'SunOS') { $pingcmd = 'ping' }

# Command line options parsing
Getopt::Long::Configure qw(posix_default require_order bundling no_ignore_case);
GetOptions(
	   'S=s' => \$sep,
	   'm=s' => \$syslogmsg,
	   'put=s' => \@putfiles,
	   'logdir=s' => \$logdir,
	   'L' => \$runlocally,
	   'ping!' => \$ping,
	   'rsh|e=s' => \$rsh,
	   'spawn=i' => \$semaphore_nb,
	   'timeout=i' => \$_o_timeout,
	   'user=s' => \$ssh_user,
	   'keyfile=s' => \$ssh_keyfile,
	   'verbose|v' => \$verbose,
	   ) or (die $!);


while (1) {
	my $arg = shift @ARGV;
	if (not defined $arg) { last }
	if ($arg eq $sep) { last }
	push @commands, $arg;
}
while (1) {
	my $arg = shift @ARGV;
	if (not defined $arg) { last }
	push @hosts, $arg;
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

if(@putfiles) {
	grep (/\.\./, @putfiles) and die "Argument putfiles contains illegal '..'";
	grep (/\s+/, @putfiles) and die "Argument putfiles contains illegal ' '";
	
}
$semaphore_nb=($semaphore_nb>$thread_max?50:$semaphore_nb);
&logverbose('',"== Thread number=$semaphore_nb");


#############################
# Sub routine to print logs
#############################

sub printlog {
	my ($verboselog, $tag, $text) = @_;
	my $now = strftime("[%Y/%m/%d_%H:%M:%S]", localtime);
	if (!$verboselog || $verbose) {
		print "$now ($tag) $text\n";
	}
	if (defined $loghandle) {
		print $loghandle "$now $text\n";
	}
}

sub logerror {
	my ($tag, $text) = @_;

	printlog(0, $tag, ">>> ERROR: $text");
}

sub logoutput {
	my ($tag, $text) = @_;

	printlog(0, $tag, $text);
}

sub lognormal {
	my ($tag, $text) = @_;

	printlog(0, $tag, ">>> $text");
}

sub logverbose {
	my ($tag, $text) = @_;

	printlog(1, $tag, ">>>>> $text");
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
		logerror($slaveid, "Can't execute command: fork: $!");
		return 300;
	}
	if ($pid == 0) {
		$pipe->writer();
		# Set pipe as stdout.
		my $stdout = \*STDOUT;
		if (not defined $stdout->fdopen($pipe->fileno, 'w')) {
			logerror($slaveid, "Can't execute command: fdopen: $!");
			exit 127;
		}
		# Shutdown a warning from Perl: it yells when something else
		# than "exit" is called after "exec".
		no warnings;
		exec $command;
		logerror($slaveid, "Can't execute command: exec: $!");
		exit 127;
	}
	$pipe->reader();
	while (<$pipe>) {
		chomp;
		logoutput($slaveid, $_);
	}
	$pipe->close;
	waitpid $pid, 0;
	my $status = $?;
	if ($status & 127) {
		logerror($slaveid, "Command killed with signal ".($status & 127));
		return 301;
	}
	return ($status >> 8);
}


sub timedrun {
	my ($timer, $command, $slaveid) = @_;
	my ($start, $end);

	logverbose($slaveid, "Begin time-bound run (max ${timer}s): $command");
	$start = time;
	my $status = &Job::Timed::runSubr($timer, \&pipedrun, @_);
	if (not defined $status) {
		logverbose($slaveid, "Job::Timed::runSubr: ".&Job::Timed::error());
		return 400;
	}
	$end = time;
	logverbose($slaveid, "End time-bound run (lasted ".($end - $start)."s): $command");
	return $status;
}

#############################
# Print BEGIN/END message on remote host
# - Argument 0/1 means begin/end
#############################
sub stamp_ce {
	my ($host, $stamp, $slaveid)=@_;
	return 0 if (not defined $syslogmsg);
	my $retval=&timedrun(30,"$rsh ".$ssh_user."@".$host." 'logger -p $logger_pri \"$syslogmsg $stamp\"'",$slaveid);
	return $retval;
}


###############################################
# Threaded routine
# Check host availability
# send script to the host, then execute it.
###############################################

sub escape {
	my ($s) = @_;

	$s =~ s/'/'\\''/g;
	$s = "'$s'";
	return $s;
}


sub dojob {
	my ($slaveid, $jobid, $job, $jobmax) = @_;
	my ($host, $command, $push) = ($job->{'host'}, $job->{'command'}, $job->{'push'});
	my $status;
	my $remotecommand;

	$SIG{'INT'} = $SIG{'TERM'} = \&Job::Timed::terminate;

	#if ($logdir) {
	#	if (not defined open $loghandle, ">>$logdir/$host") {
	#		&logerror($slaveid, "== WARNING: Cannot open '$logdir/$host' for writing: $!");
	#	} else {
	#		# Shared among multiple process, so disable buffering.
	#		$loghandle->autoflush;
	#	}
	#}

	if (not $host) {
		lognormal($slaveid, "(job:$jobid/$jobmax) local: $command");
		$status = timedrun($timeout, $command, $slaveid);
		goto OUT;
	}

	lognormal($slaveid, "(job:$jobid/$jobmax) \@$host: $command");
	if ($ping) {
		if (timedrun(5, "$pingcmd $host >/dev/null 2>&1", $slaveid)) {
			logerror($slaveid, "Cannot ping '$host'");
			$status = 500;
			goto OUT;
		}
	}

	if ($push) {
		# This doesn't support script names with spaces in it, enclosed in quotes.
		# But anyway, which fool would use such a thing?
		$command =~ m/^(\S+)/;
		my $localscript = $1;
		my $remotescript = $localscript;
		$remotescript =~ s{.*/}{};
		$remotescript = "/var/tmp/$remotescript".rand(10000);
		lognormal($slaveid, "Pushing '$localscript' to $host as '$remotescript'");
		if (timedrun(30, "$rcp $localscript $ssh_user\@$host:$remotescript", $slaveid)) {
			logerror($slaveid, "Cannot push '$localscript' to $host");
			goto OUT;
		}
		$command =~ s/^\S+//;
		$command = "chmod +x $remotescript; $remotescript $command; rm -f $remotescript";
	}

	$command = escape($command);
	$status = timedrun($timeout, "$rsh $ssh_user\@$host $command 2>&1", $slaveid);
	#print "$rsh $ssh_user\@$host $command 2>&1\n";
	if ($status) {
		logerror($slaveid, "Return status $status");
		goto OUT;
	}

OUT:
	#if (defined $loghandle) { close $loghandle }
	return $status;
}


############################################################
#			  Actually do the work:
# loop over machine list and start as many threads as needed
# threads are detached to be sure to free resources...
############################################################

my @jobs;

if (@hosts == 0) { push @hosts, '' }
foreach my $host (@hosts) {
	my $push = 0;
	foreach my $command (@commands) {
		my %args = ();

		if ($command eq '+s') {
			$push = 1;
			next;
		}
		if ($push and not -f $command) { die "No such file: $command" }
		$args{'host'} = $host;
		$args{'command'} = $command;
		$args{'push'} = $push;
		$push = 0;
		push @jobs, \%args;
	}
}

foreach my $args (@jobs) {
	if ($args->{'host'}) {
		print '@'.$args->{'host'}.': ';
	} else {
		print 'locally: ';
	}
	if ($args->{'push'}) {
		print 'push and run '.$args->{'command'}."\n";
	} else {
		print $args->{'command'}."\n";
	}
}

$SIG{'INT'} = $SIG{'TERM'} = sub {
	Job::Parallel::terminate();
	if (!Job::Parallel::isChild()) { exit 0 }
};

Job::Parallel::run($semaphore_nb, \&dojob, scalar @hosts, @jobs);


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
	-s|--script=<filename>
		--scriptoptions=<options>
	-s|<syslog message>
	--put=<filename|dirname>
	--logdir=<log directory>
	-e|--rsh=<rsh|ssh>
	--spawn=<nthreads>
	-L
	-N 
	--timeout=<timeout>					time to wait for command execution in seconds
	--user=<ssh username>		   Username used to make ssh connections
	--keyfile=<ssh private key file>		Private key used to make ssh connections

Exemples:
	./multremsh.pl --spawn=10 -e ssh --logdir=logs/ -m "REL 162663: install jrockit 1.4.2_13" -t gigaprod05 \
				   -t gigaprod06 -t pnlgui001 -t pnlgui002 -s scripts/Linux.jrockit-jdk1.4.2_12.sh \
				   --put=scripts/Linux.jrockit-jdk1.4.2_12.tar.gz

=head1 OPTIONS

=over
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

=item B<--put>=I<filename>

	Files to be put in /tmp on destination host. Transfer is done using either
rcp -r or scp -r so it works for directories. Any trainling / is stipped from
the argument to avoid undocumented copy troubles in case of directories. You can
use several --put options to build a list of files/directories.

=item B<--logdir>=I<log directory>

	Specify a log directory in which to put target logs. One file per target.
Log Filename is the hostname of the target.

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

