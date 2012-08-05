#!/usr/bin/perl -w
#
# 2008-2012 Jeremie Le Hen <jeremie@le-hen.org>
#
# Based on multremsh.pl:
# 2004 Vincent Haverlant <vincent.haverlant@sgcib.com> 
# $Id: multremsh.pl 73 2007-11-12 13:12:23Z vhaverla $

# Put there variables here as they are used in usage().
my $parallelism = 5; 
my $pingtimeout = 5;
my $scptimeout = 60;
my $timeout = 300;
my $connecttimeout = 10;

sub usage {
	my $me = $0;

	$me =~ s{.*/}{};
	print <<EOF;
Usage:

* Detailled informations and example for each command:
    $me help push
    $me help pull
    $me help exec
    $me help pushnexec

* Remote commands:
    $me [options] push <file> [in <dir>] on <host> [host ...]
    $me [options] pull <file> [from <dir>] on <host> [host ...]
    $me [options] exec <command> [in <dir>] on <host> [host ...]
    $me [options] pushnexec <command> [in <dir>] on <host> [host ...]

* Local commands:
    $me [options] exec <command> locally [for <host> [host ...]]

* Common options:
    -a           Append to log files.
    -l <logdir>	 Logs everything in <logdir>/ (will be created)
    -n <number>	 Number of commands to run simultaneously, default: $parallelism
    -q           Decrease verbosity (use once to show only errors).
    -t <seconds> Timeout when running a command, default: $timeout
    -T           Do not tag log lines with the host being processed
    -U           Do not issue a summary upon completion (still logged if -l)
    -v           Increase verbosity (use twice to show command output).
    -W <size>    Set window width (for summary)

* Remote command options:
    -C <seconds> Connect timeout for ssh/scp, default: $connecttimeout
    -k <keyfile> Use <keyfile> when using ssh
    -p <seconds> Ping timeout when testing host, disable with 0, default: $pingtimeout
    -S <seconds> Timeout when scp'ing a file, default: $scptimeout
    -u <user>	 Use <user> when using ssh, default: \$LOGNAME

* Local command options:
    -s <string>	 Substitute <string> in the command string with the current
                 hostname, default: %ARG%

* Notes:

  Reading from stdin
    For each <file>/<command> or <host> parameter, you can use either "-" to
    read the list from stdin or "file:</path/to/file>" to read from a file
    directly.  In both case empty lines and comment lines (starting with #)
    will be skipped.  Obviously you cannot use "-" for both <file>/<command>
    and <host> simultaneously.

  Commands
    The syntax expect <command> to be passed as a single argument.  This means
    that you must enclose it in quotes if it needs arguments.

  Logfiles
    When using the -l option, two files are created by commands:
      .log file contains parallel.pl messages and command stdout/stderr;
      .out file contains command stdout/stderr.
    If the if the command failed somewhere, the following file is created:
      .fail file contains the reason of the failure.

  Status field in summary
    A status of "-" means that an error occured, but coming from $me\'s
    internals instead of the program intended to run.  In that case, the
    line count in 0 and the last line field is the error.  This includes a
    failed ping(8) or scp(8).  If you want to know what's happened with
    these commands, you have to either look at the .log file if -l was used
    or use -vv.
EOF
	exit 0;
}

sub help_push {
	my $me = $0;

	$me =~ s{.*/}{};
	print <<EOF;
Synopsys:
  $me push <file> [in <dir>] on <host> [host ...]

Description:
  The ``push'' command will just transfer <file> on each <host>, optionally
  in the specified <dir>.  If no <dir> is specifed, the user's home
  directory is used.

  This command is only meaningful in a remote context.  Pushing files
  locally is useless.

Examples:
  $me -n 2 push 119744-11.gz in /var/tmp on host1 host2

  cat hosts.txt | xargs $me -n 10 push /etc/resolv.conf in /etc on

EOF
	exit 0;
}

sub help_pull {
	my $me = $0;

	$me =~ s{.*/}{};
	print <<EOF;
Synopsys:
  $me pull <file> [in <dir>] on <host> [host ...]

Description:
  The ``pull'' command will just transfer <file> from each <host>, optionally
  in the local specified <dir>.  The suffix ".host" will be appended to the
  name of the file locally.  If no <dir> is specifed, the current directory
  is used.

  This command is only meaningful in a remote context.  Pull files
  locally is useless.

Examples:
  $me -n 2 pull /etc/redhat-release in /var/tmp on host1 host2

EOF
	exit 0;
}

sub help_exec {
	my $me = $0;

	$me =~ s{.*/}{};
	print <<EOF;
Synopsys:
  Remote: $me exec <command> [in <dir>] on <host> [host ...]
  Local: $me exec <command> locally for <arg> [arg ...]

Description:
  When used in its remote version, the ``exec'' command will execute <command>
  on each <host>, optionally from the specified <dir>.  <command> may be a
  full-fledged shell command, including pipes, quotes and even scripting.  Just
  be careful to enclose it in quotes, so it is considered as a single argument.
  If no <dir> is specifed, the user's home directory is used.

  When used in its local version, the ``exec'' command will execute <command>
  for each <arg>.  <command> may be a full-fledged shell command, including
  pipes, quotes and even scripting.  Just be careful to enclose it in quotes,
  so it is considered as a single argument.

  Every "%ARG%" string in the command will be replaced by the current <arg>.
  This string can be changed with the -s option.

Examples:
  $me -n 2 exec "echo -n '%ARG% is: '; uname -s" on host1 host2 host3 host4

  $me -n 2 exec "grep ^%ARG%: /etc:passwd" locally for root jlehen

EOF
	exit 0;
}

sub help_pushnexec {
	my $me = $0;

	$me =~ s{.*/}{};
	print <<EOF;
Synopsys:
  $me pushnexec <file> [in <dir>] on <host> [host ...]

Description:
  The ``pushnexec'' command is a combination of the ``push'' and ``exec''
  commands (incredible, isn't it?).  <command> can include arguments, in
  which case only first word will be pushed.  Note that the pushed file
  will be set executable before being executed and will be removed after
  the execution.

  This command is only meaningful in a remote context.  Pushing files
  locally is useless.

Examples:
  $me pushnexec "./doit.sh --yes --force" on host1 host2

EOF
	exit 0;
}

use strict;
use Getopt::Long;
use File::Basename;
use Time::HiRes qw(gettimeofday tv_interval);
use POSIX qw(strftime mkdir :sys_wait_h);
use Errno qw(:POSIX);
use IO::Pipe;
use IO::Select;

my $dirname;
$dirname = $0;
$dirname =~ s{\/?[^/]+$}{};
if ($dirname eq '') { $dirname = '.' }
unshift @INC, $dirname;
unshift @INC, "$dirname/p5-Job";
if (not require Job::Parallel) { die "Cannot find Job::Parallel" }
if (not require Job::Timed) { die "Cannot find Job::Timed" }

# Initialisation and default values
my @argv0 = @ARGV;
my $argc_noopt = 0;
my $maxoutputlevel = 0;
my $hosttag = 1;
my $appendlog = 0;
my $summary = 1;
my $ssh_user = $ENV{'LOGNAME'};
my $ssh_keyfile;
my $subst = $ENV{'SUBST'} ? quotemeta ($ENV{'SUBST'}) : '\%ARG\%';
my $logdir = '';
my $loghandle;
my $outhandle;
my $failfile;
my $ssh_opts = '-o BatchMode=yes -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o NumberOfPasswordPrompts=0';
my $mode = '>';
my $precision;
my $os;
my $pingcmd;
my $sshcmd;
my $scpcmd;
my $width;

$|=1;

Getopt::Long::Configure qw(posix_default require_order bundling no_ignore_case);
GetOptions(
	'a' => sub { $appendlog = 1 },
	'v' => sub { $maxoutputlevel++ },
	'q' => sub { $maxoutputlevel-- },
	'T' => sub { $hosttag = 0 },
	'U' => sub { $summary = 0 },
	'l=s' => \$logdir,
	'n=i' => \$parallelism,
	'p=i' => \$pingtimeout,
	't=i' => \$timeout,
	'S=i' => \$scptimeout,
	'C=i' => \$connecttimeout,
	'u=s' => \$ssh_user,
	'k=s' => \$ssh_keyfile,
	's=s' => \$subst,
	'W=i' => \$width,
	'h' => \&usage,
) or (die $!);

$argc_noopt = @ARGV;
$os = `uname -s`;
chomp $os;
$pingcmd = 'ping -c 2';
if ($os eq 'SunOS') { $pingcmd = 'ping' }

if ($ssh_keyfile) { $ssh_opts .= ' -i '.$ssh_keyfile }
if ($connecttimeout > 0) { $ssh_opts .= " -o ConnectTimeout=".$connecttimeout }
$sshcmd = "ssh -n $ssh_opts";
$scpcmd = "scp $ssh_opts";
if ($appendlog) { $mode = '>>' }

if ($logdir) {
	if (-d $logdir) {
		warn "Directory '$logdir' already exists, non-overwritten files will stay";
	} elsif (not mkdir $logdir, 0700) {
		die "Cannot create directory '$logdir': $!";
	}
}

# Precision needed to print all thread ids using the same width.
$precision = sprintf ("%d", log($parallelism) / log(10) + 1);

# =-=-=-=-=-=-
# Log routines
# =-=-=-=-=-=-

sub printlog {
	my ($level, $tag, $text) = @_;
	my $now = strftime("[%Y/%m/%d_%H:%M:%S]", localtime);
	if ($level <= $maxoutputlevel) { print "$now ($tag) $text\n" }
	if (defined $loghandle) { print $loghandle "$now ($tag) $text\n" }

	# Late creation of the fail file on error.
	if ($level < 0 and $logdir) {
		my $failhandle;
		if (open $failhandle, '>', $failfile) {
			print $failhandle "$now ($tag) $text\n";
			close $failhandle;
		}
	}
}

sub logerror {
	my ($tag, $text) = @_;

	printlog(-1, $tag, ">>> ERROR: $text");
}

sub lognormal {
	my ($tag, $text) = @_;

	printlog(0, $tag, ">>> $text");
}

sub logdetail {
	my ($tag, $text) = @_;

	printlog(1, $tag, ">>>>> $text");
}

sub logoutput {
	my ($tag, $text) = @_;

	if (defined $outhandle) { print $outhandle "$text\n" }
	printlog(2, $tag, $text);
}


# =-=-=-=-=-=-=
# Core routines
# =-=-=-=-=-=-=

#
# This function creates 2 pipes, forks and sets them as stdout and stderr
# before running the child.  The parent process reads the pipes and writes
# it to the log.
#
# Returns a list reference [ $status, $elapsed, $linecount, $lastline ].
#
# $status is always defined.  If we cannot fork, then the $status will
# be 999 and the appropriate log is issued; if the command has been killed with
# a signal, then $status is 1000 + signum.  Also, $linecount is set to 0 and
# $lastline contains the error message, so it can be displayed in the summary.
#
# If the command has been killed with a signal, then $status is
# 1000 + signum.
#
# For pratical reasons, if the command died because of a signal, $status
# will be "sig".$signal.
sub pipedrun {
	my ($timer, $command, $tag, $descwhat) = @_;
	my ($linecount, $lastline);

	my $tv0 = [ gettimeofday() ];
	$linecount = 0;
	$lastline = '';
	# It would be easier to use open()'s pipe feature, but we wouldn't
	# be able to get the return status of the command.
	my $outpipe = new IO::Pipe;
	my $errpipe = new IO::Pipe;
	my $pid = fork;
	if (not defined $pid) {
		$lastline = "Cannot fork: $!";
		logerror($tag, $lastline);
		return (999, 0, 0, $lastline);
	}
	if ($pid == 0) {
		$outpipe->writer();
		$errpipe->writer();
		# Set pipe as stdout.
		my $stdout = \*STDOUT;
		my $stderr = \*STDERR;
		if (not defined $stdout->fdopen($outpipe->fileno, 'w')) {
			logerror($tag, "Can't execute command: fdopen for stdout: $!");
			exit 127;
		}
		if (not defined $stderr->fdopen($errpipe->fileno, 'w')) {
			logerror($tag, "Can't execute command: fdopen for stderr: $!");
			exit 127;
		}
		# Shutdown a warning from Perl: it yells when something else
		# than "exit" is called after "exec".
		no warnings;
		exec $command;
		logerror($tag, "Cannot exec: $!");
		exit 127;
	}

	$outpipe->reader();
	$errpipe->reader();
	$outpipe->blocking(0);
	$errpipe->blocking(0);
	my $select = IO::Select->new();
	$select->add($outpipe);
	$select->add($errpipe);
	# $lastturn is here so we do a last check on the pipes when the
	# child has exited, so we do not miss any output.
	my $lastturn = 0;
	my $status;
	my $laststatus;
	my @zombies = ();
	# %halfline is used when we didn't get a full line.  So instead of
	# logging a halfline, buffer it and try to fetch the leftover.
	# This is done for each filehandle.
	my %halfline;
	my $pfx;
	while (1) {
		my @ready = $select->can_read();
		foreach my $fh (@ready) {
			$pfx = '';
			my $line;
			if ($fh == $errpipe) { $pfx = 'ERR: ' }
			while (1) {
				$line = <$fh>;
				if (not defined $line) { last }
				if (defined $halfline{$fh}) {
					$line = $halfline{$fh} . $line;
					$halfline{$fh} = undef;
				}
				if (not chomp $line) {
					$halfline{$fh} = $line;
					next;
				}
				# XXX Quite ugly, but sometimes needed.
				$line =~ s/[\r]$//;
				$linecount++;
				$lastline = "$pfx$line";
				logoutput($tag, $lastline);
			}
		}
		if ($lastturn) { last }
		while (1) {
			my $pid2 = waitpid -1, WNOHANG;
			if ($pid2 == $pid) { $status = $? }
			if ($pid2 > 0) {
				$laststatus = $?;
				push @zombies, $pid2;
				next;
			}
			if ($pid2 == 0) { last }

			# $pid2 < 0
			if ($! != ECHILD) { next }
			$select->remove($outpipe);
			$select->remove($errpipe);
			$outpipe->close();
			$errpipe->close();
			$lastturn = 1;
			last;
		}
	}
	# Better log a half line than lose it.
	foreach my $fh ($outpipe, $errpipe) {
		if ($fh == $errpipe) { $pfx = 'ERR: ' }
		if (defined $halfline{$fh}) {
			$linecount++;
			$lastline = "$pfx$halfline{$fh}";
			logoutput($tag, $lastline);
			$halfline{$fh} = undef;
		}
	}

	if (not defined $status) {
		logerror($tag, "WEIRD BEHAVIOUR DETECTED, child PID $pid vanished (reaped zombies: @zombies)");
		$status = $laststatus;
	}

	my $elapsed = sprintf ("%.3f", tv_interval($tv0));
	if ($status & 127) {
		logerror($tag, "$descwhat killed with signal ".($status & 127));
		return [ 1000 + ($status & 127), $elapsed, $linecount, $lastline ];
	}
	return [ $status >> 8, $elapsed, $linecount, $lastline ];
}


#
# Returns a list reference [ $status, $elapsed, $linecount, $lastline ].
#
# $status is always defined.  If Job::Timed::runSubr() returns an interal
# error, then $status will be 999 and the appropriate log is issued;
# also, $linecount is set to 0 and $lastline contains the error message,
# so it can be displayed in the summary.
sub timedrun {
	my ($timer, $command, $tag, $descwhat) = @_;
	my ($result, $status, $elapsed, $lastline);

	my $tv0 = [ gettimeofday() ];
	$result = Job::Timed::runSubr($timer, \&pipedrun, @_);
	if (defined $result) { return $result }

	$elapsed = sprintf ("%.3f", tv_interval($tv0));

	my $error = Job::Timed::status();
	if ($error >= 0) {
		die "ASSERTION FAILED: ($tag) Job::Timed::runSubr() ".
		    "reported an error but Job::timed::status() ".
		    "returned a non-negative value $error";
	}
	$status = $error;

	if ($error == -1) {
		$lastline = "Job::Timed::runSubr: ".Job::Timed::error().
		    " (after ${elapsed}s)";
		logerror($tag, $lastline);
	} elsif ($error == -2) {
		$lastline = "$descwhat exhausted its allocated time (${elapsed}s)";
		logerror($tag, $lastline);
	} elsif ($error == -3) {
		$lastline = "$descwhat as been interrupted after ${elapsed}s";
		logerror($tag, $lastline);
	}
	return [ 999, $elapsed, 0, $lastline ];
}


# Enclose a command in single quotes in order to execute it through ssh.
sub escape {
	my ($s) = @_;

	$s =~ s/'/'\\''/g;
	$s = "'$s'";
	return $s;
}

#
# This function contains the intelligence.
sub dojob {
	my ($slaveid, $jobid, $job, $jobmax) = @_;
	my $cmdid = $job->{'cmdid'};
	my $where = $job->{'where'};
	my $action = $job->{'action'};
	my $command = $job->{'command'};
	my $host = $job->{'host'};
	my $dir = $job->{'dir'};
	my ($result, $status, $duration, $linecount, $lastline);
	my $realcommand;
	my $savedhandle;
	my $tag;

	$SIG{'INT'} = $SIG{'TERM'} = \&Job::Timed::terminate;

	$duration = 0;
	$linecount = 0;
	$lastline = '';

	$tag = sprintf ("%0*d", $precision, $slaveid);
	if ($hosttag and $host) { $tag = "$tag:$host" }

	if ($logdir) {
		my $logfile = $host ? $host : $jobid;

		# Don't care if they do not exist.
		unlink  "$logdir/$logfile.fail";
		if (not $appendlog) {
			unlink  "$logdir/$logfile.log";
			unlink  "$logdir/$logfile.out";
		}

		if (not open $loghandle, $mode, "$logdir/$logfile.log") {
			$status = 999;
			$lastline = "Cannot open '$logdir/$logfile.log' for writing: $!";
			logerror($tag, $lastline);
			goto OUT;
		}
		if (not open $outhandle, $mode, "$logdir/$logfile.out") {
			$status = 999;
			$lastline = "Cannot open '$logdir/$logfile.out' for writing: $!";
			logerror($tag, $lastline);
			goto OUT;
		}
		$failfile = "$logdir/$logfile.fail";
		# Shared among multiple process, so disable buffering.
		$loghandle->autoflush(1);
		$outhandle->autoflush(1);
	}

	# Local run is straightforward.
	if ($where eq 'local') {
		# Sanity checks after the parser ensured that the action is 'exec'.
		lognormal($tag, "(job:$jobid/$jobmax) exec local: $command");
		if (defined $outhandle) {
			print $outhandle "# exec local: $command\n";
		}
		$result = timedrun($timeout, $command, $tag, "Command");
		($status, $duration, $linecount, $lastline) = @$result;
		if (defined $outhandle) {
			print $outhandle "# exec local: status: $status; duration: ${duration}s; output: $linecount lines\n";
		}
		if ($status > 0) {
			logerror($tag, "Command returned status $status after ${duration}s and produced $linecount lines")
		} elsif ($status == 0) {
			logdetail($tag, "Command returned successfully after ${duration}s and produced $linecount lines");
		}
		goto OUT;
	}

	lognormal($tag, "(job:$jobid/$jobmax) $action \@$host: $command");

	# We don't want to saved ping(8) or scp(8) output into the .out file.
	$savedhandle = $outhandle;
	$outhandle = undef;

	if ($pingtimeout > 0) {
		logdetail($tag, "Pinging $host");
		# Don't log this in the output file.
		$result = timedrun($pingtimeout, "$pingcmd $host", $tag, "ping(8) \@$host");
		$status = $result->[0];
		if ($status != 0) {
			$status = 999;
			$lastline = "Cannot ping '$host'";
			logerror($tag, $lastline);
			goto OUT;
		}
	}

	$dir =~ s{/$}{};

	if ($action eq 'pull') {
		my $localfile = $command;
		$localfile =~ s{.*/}{};
		$localfile .= ".$host";

		if (not $dir) { $dir = '.' }
		logdetail($tag, "Pulling '$command' from $host into $dir/$localfile");
		$result = timedrun($scptimeout, "$scpcmd $ssh_user\@$host:$command $dir/$localfile", $tag, "scp(1) \@$host");
		$status = $result->[0];
		if ($status != 0) {
			$status = 999;
			$lastline = "Cannot pull '$command' to $host";
			logerror($tag, $lastline);
		}
		goto OUT;
	}

	$realcommand = $command;
	if ($action eq 'push' or $action eq 'pushnexec') {
		# This doesn't support script names with spaces in it, enclosed in quotes.
		# But anyway, which fool would use such a thing?
		$command =~ m/^(\S+)/;
		my $localfile = $1;
		my $remotefile = $localfile;
		$remotefile =~ s{.*/}{};

		if ($dir) {
			$remotefile = "$dir/$remotefile";
		} else {
			$remotefile = "./$remotefile";
		}
		logdetail($tag, "Pushing '$localfile' to $host as '$remotefile'");
		$result = timedrun($scptimeout, "$scpcmd $localfile $ssh_user\@$host:$remotefile", $tag, "scp(1) \@$host");
		$status = $result->[0];
		if ($status != 0) {
			$status = 999;
			$lastline = "Cannot push '$localfile' to $host";
			logerror($tag, $lastline);
			goto OUT;
		}

		if ($action eq 'push') { goto OUT }

		# Only meaningful for 'pushnexec'.
		$realcommand =~ s/^\S+//;
		$realcommand = "chmod +x $remotefile; $remotefile $realcommand; rm -f $remotefile";
	}

	$outhandle = $savedhandle;
	$savedhandle = undef;

	if (defined $outhandle) {
		print $outhandle "# $action \@$host: $command\n";
	}
	if ($dir) { $realcommand = "cd $dir; $realcommand" }

	$realcommand = escape($realcommand);
	logdetail($tag, "Running command");
	$result = timedrun($timeout, "$sshcmd $ssh_user\@$host $realcommand", $tag, "Command \@$host");
	$status = $result->[0];
	$duration = $result->[1];
	$linecount = $result->[2];
	$lastline = $result->[3];

	if (defined $outhandle) {
		print $outhandle "# $action \@$host: status: $status; duration: ${duration}s; output: $linecount lines\n";
	}
	if ($status < 0) { goto OUT } # Error message already logged
	if ($status > 0) {
		logerror($tag, "Command \@$host returned status $status after ${duration}s and produced $linecount lines");
	} else {
		logdetail($tag, "Command \@$host returned successfully after ${duration}s and produced $linecount lines");
	}

OUT:
	if (defined $savedhandle) { $outhandle = $savedhandle }
	if (defined $loghandle) { close $loghandle }
	if (defined $outhandle) { close $outhandle }
	return [ $host ? $host : "", $cmdid, $status, $duration, $linecount, $lastline ];
}


# =-=-=-=-=-=-=-=-=-=
# Command-line parser
# =-=-=-=-=-=-=-=-=-=
#
# Here is the deterministic finite state machine of the command-line grammar.
# It is really easy to implement once you have this.
#
#      .->"pull"-.
#     /           \                                                  [host]<-+
#    | .->"push"-. |           "in"-->S6-->[dir]-->S7-->"on"           |     |
#    |/           \|            ^                        |             |  .--+
#    |             v            |                        v             v /
# ->S0---"exec"--->S1--[cmd]-->S2---------->"on"-------->S4--[host]---S5-->
#    |             ^           /|                        ^
#    |\           /           v |                        |
#      "pushnexec"              +->"locally"->S3-->"for"-+
#                                             |
#                                             v

use constant {
	S0 => 0, S1 => 1, S2 => 2, S3 => 3,
	S4 => 4, S5 => 5, S6 => 6, S7 => 7
};

my @expected = (
	[ 'pull', 'push', 'exec', 'pushnexec' ],	# S0
	[ '<command>', '<file>' ],			# S1
	[ 'on', 'locally', 'in' ],			# S2
	[ 'for' ],					# S3
	[ '<host>' ],					# S4
	[ ],						# S5
	[ '<dir>' ],					# S6
	[ 'on' ]					# S7
);
my $i;
my $state = S0;
my ($action, $what, @hosts, $where, $dir);

if (not $ARGV[0]) { usage() }
if ($ARGV[0] eq 'help') {
	if ($ARGV[1] && $ARGV[1] eq 'exec') { help_exec() }
	if ($ARGV[1] && $ARGV[1] eq 'push') { help_push() }
	if ($ARGV[1] && $ARGV[1] eq 'pull') { help_pull() }
	if ($ARGV[1] && $ARGV[1] eq 'pushnexec') { help_pushnexec() }
	if ($ARGV[1]) {
		print STDERR "ERROR: Unexpected '$ARGV[1]'\n";
	}
	usage();
}

for ($i = 0; $i < @ARGV; $i++) {
	my $w = $ARGV[$i];

	if ($state == S0) {
		if ($w eq 'exec' or $w eq 'pushnexec' or $w eq 'pull' or
		    $w eq 'push') {
			$action = $w;
			$state = S1;
			next;
		}
		die "Expecting ".join ('/', @{$expected[S0]})." at word $i ('$w')";
	}
	if ($state == S1) {
		$what = $w;
		$state = S2;
		next;
	}
	if ($state == S2) {
		if ($w eq "in") {
			$where = 'remote';
			$state = S6;
			next;
		}
		if ($w eq 'on') {
			$where = 'remote';
			$state = S4;
			next;
		}
		if ($w eq 'locally') {
			$where = 'local';
			$state = S3;
			next;
		}
		die "Expecting ".join ('/', @{$expected[S2]})." at word $i ('$w')";
	}
	if ($state == S3) {
		if ($w eq 'for') {
			$state = S4;
			next;
		}
		die "Expecting ".join ('/', @{$expected[S3]})." at word $i ('$w')";
	}
	if ($state == S4 or $state == S5) {
		push @hosts, $w;
		$state = S5;
		next;
	}
	if ($state == S6) {
		$dir = $w;
		$state = S7;
		next;
	}
	if ($state == S7) {
		if ($w eq 'on') {
			$state = S4;
			next;
		}
		die "Expecting ".join ('/', @{$expected[S7]})." at word $i ('$w')";
	}
}

if ($state == S2) {
	$where = 'local';
	$state = S3;
}
if (not defined $where) {
	die 'ASSERTION FAILED: $where should be either "local" or "remote"';
}

if ($state != S3 and $state != S5) {
	die "Expecting ".join ('/', @{$expected[$state]})." at word $i";
}
if ($where eq 'local' and $action ne 'exec' and $action ne 'readnexec') {
	die "Cannot push/pull locally";
}
if ($what eq '-') {
	foreach (@hosts) {
		if ($_ ne '-') { next }
		die 'Cannot use "-" for both <file>/<command> and <host>.';
	}
}

my @commands = ();
if ($what eq '-' or $what =~ m/^file\:/) {
	my $fh;
	if ($what eq '-') {
		$fh = \*STDIN;
	} else {
		$what =~ m/^file\:(.*)/;
		my $file = $1;
		if (not open ($fh, '<', $file)) { die "$file: $!" }
	}
	my $line;
	while (defined ($line = <$fh>)) {
		chomp $line;
		if ($line =~ m/^\s*$/) { next }	
		if ($line =~ m/^\s\#/) { next }	
		push @commands, $line;
	}
	if ($what ne '-') { close $fh }
} else {
	push @commands, $what;
}

my @hosts2;
foreach my $host (@hosts) {
	if ($host ne '-' and $host !~ m/^file\:/) {
		push @hosts2, $host;
		next;
	}
	my $fh;
	if ($host eq '-') {
		$fh = \*STDIN;
	} else {
		$host =~ m/^file\:(.*)/;
		my $file = $1;
		if (not open ($fh, '<', $file)) { die "$file: $!" }
	}
	my $line;
	while (defined ($line = <$fh>)) {
		chomp $line;
		if ($line =~ m/^\s*$/) { next }	
		if ($line =~ m/^\s\#/) { next }	
		push @hosts2, $line;
	}
	if ($host ne '-') { close $fh }
}
@hosts = @hosts2;

my @jobs;
if (@hosts > 0) {
	my %job = ();

	$i = 0;
	if ($where eq 'local') {	# Substitution
		foreach my $cmd (@commands) {
			$i++;
			foreach my $host (@hosts) {
				my $job = {};
				my $realcmd = $cmd;
				$realcmd =~ s/$subst/$host/g;
				$job->{'cmdid'} = $i;
				$job->{'where'} = $where;
				$job->{'action'} = $action;
				$job->{'command'} = $realcmd;
				$job->{'host'} = $host;
				push @jobs, $job;
			}
		}
	} else {			# 'remote', Combination
		foreach my $cmd (@commands) {
			$i++;
			foreach my $host (@hosts) {
				my $job = {};
				my $realcmd = $cmd;
				$realcmd =~ s/$subst/$host/g;
				$job->{'cmdid'} = $i;
				$job->{'where'} = $where;
				$job->{'action'} = $action;
				$job->{'command'} = $realcmd;
				$job->{'host'} = $host;
				$job->{'dir'} = $dir ? $dir : '';
				push @jobs, $job;
			}
		}
	}
} else {
	if ($where ne 'local') {
		die 'ASSERTION FAILED: remote command with no hosts';
	}
	
	$i = 0;
	foreach my $cmd (@commands) {
		my $job = {};
		$i++;
		$job->{'cmdid'} = $i;
		$job->{'action'} = $action;
		$job->{'command'} = $cmd;
		$job->{'host'} = '';
		push @jobs, $job;
	}
}

$SIG{'INT'} = $SIG{'TERM'} = sub {
	Job::Parallel::terminate();
	#if (not Job::Parallel::isChild()) { exit 0 }
};

# Results in an array of [ $host , $cmdid, $status, $duration, $linecount, $lastline ]
my @results = Job::Parallel::run($parallelism, \&dojob, scalar (@jobs), @jobs);

if (not defined $results[0]) {
	die 'Job::Parallel::run(): '.Job::Parallel::error();
}
if (not $logdir and not $summary) { exit 0 }

#
# Compute "Host/Id" column size and structurize results.
my %results;
my $hostsize = 4;	# length("Host") == 4
my $idsize = 2;		# length("Id") == 2
foreach my $r (@results) {
	my ($host, $id) = ($r->[0], $r->[1]);
	if (length ($host) > $hostsize) { $hostsize = length ($host) }
	if (length ($id) > $idsize) { $idsize = length ($id) }

	if (not defined $results{$host}) { $results{$host} = {} }
	$results{$host}->{$id} = $r;
}

#
# Try to determine terminal width.
if (not defined $width) {
	$width = 80;
	{
		# XXX This is too ugly.  Thanks Perl!
		local $SIG{__WARN__} = sub {};
		eval "require 'sys/ioctl.ph'";
	}
	if (not $@) {
		my $winsize;
		if (defined (&TIOCGWINSZ) and ioctl (STDOUT, (&TIOCGWINSZ), $winsize='')) {
			$width = (unpack ('S4', $winsize))[1];
		}
	}
}

my @hostorder;
if (@hosts > 0) {
	@hostorder = @hosts;		# Keep order given by user.
} else {
	@hostorder = keys %results;	# There should be only one: "".
}

if ($logdir && $summary) {
	if (not $appendlog) { unlink "$logdir/SUMMARY" }
	if (not open $loghandle, $mode, "$logdir/SUMMARY") {
		logerror("summary", "Cannot open '$logdir/SUMMARY' for writing: $!");
		if (not $summary) { exit 0 }
	}
	my $argc_opt = @argv0 - $argc_noopt;
	print $loghandle "# $0 ".join (' ', @argv0[0..($argc_opt - 1)])."\n";
	foreach (@argv0[$argc_opt..$#argv0]) { print $loghandle "#\t$_\n" }
}

print "\n";
my $line;
my $linesize;
if (@hosts > 0 && @commands > 1) {
	$line = sprintf("%*s/%-*s  %5s  %7s  %7s  %-s\n",
	    $hostsize, "Host", $idsize, "Id", "Exit", "Runtime", "# Lines", "Last line");
	$linesize = $width - ($hostsize - 1 - $idsize) - 2 - 5 - 2 - 7 - 2 - 7 - 2 - 1;
} elsif (@hosts > 0 && @commands == 1) {
	$line = sprintf("%-*s  %5s  %7s  %7s  %-s\n",
	    $hostsize, "Host",  "Exit", "Runtime", "# Lines", "Last line");
	$linesize = $width - $hostsize - 2 - 5 - 2 - 7 - 2 - 7 - 2;
} elsif (@hosts == 0 && @commands > 1) {
	$line = sprintf("-*s  %5s  %7s  %7s  %-s\n",
	    $idsize, "Id", "Exit", "Runtime", "# Lines", "Last line");
	$linesize = $width - $idsize - 2 - 5 - 2 - 7 - 2 - 7 - 2;
}

if (defined $loghandle) { print $loghandle $line }
if ($summary) { print $line }
$line = ("-" x ($width - 1)) . "\n";
if (defined $loghandle) { print $loghandle $line }
if ($summary) { print $line }

foreach my $host (@hostorder) {
	for my $id (sort keys %{$results{$host}}) {
		my $result = $results{$host}->{$id};
		if (not defined $result) { next } # We've been interrupted
		my $status = $result->[2];
		if ($status == 999) { $status = "-" }
		elsif ($status >= 1000) { $status = "sig" . ($status - 1000) }

		if (@hosts > 0 && @commands > 1) {
			$line = sprintf("%*s/%-*s  %5s  %7s  %7s  %s\n",
			    $hostsize, $host, $idsize, $id, $status, $result->[3], $result->[4],
			    substr ($result->[5], 0, $linesize));
		} elsif (@hosts > 0 && @commands == 1) {
			$line = sprintf("%-*s  %5s  %7s  %7s  %s\n",
			    $hostsize, $host, $status, $result->[3], $result->[4],
			    substr ($result->[5], 0, $linesize));
		} elsif (@hosts == 0 && @commands > 1) {
			$line = sprintf("%-*s  %5s  %7s  %7s  %s\n",
			    $idsize, $id, $status, $result->[3], $result->[4],
			    substr ($result->[5], 0, $linesize));
		}
		if (defined $loghandle) { print $loghandle $line }
		if ($summary) { print $line }
	}
}


if (defined $loghandle) { close $loghandle }
