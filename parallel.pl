#!/usr/bin/perl -w
#
# 2008-2010 Jeremie Le Hen <jeremie.le-hen@sgcib.com>
#
# Based on multremsh.pl:
# 2004 Vincent Haverlant <vincent.haverlant@sgcib.com> 
# $Id: multremsh.pl 73 2007-11-12 13:12:23Z vhaverla $

sub usage {
	my $me = $0;

	$me =~ s{.*/}{};
	print <<EOF;
Usage:
* Detailled informations on each command:
    $me help <'push'|'exec'|'pushnexec'|'readnexec'>

* Remote commands:
    $me [options] push <file> [in <dir>] on <host> [host ...]
    $me [options] pull <file> [from <dir>] on <host> [host ...]
    $me [options] exec <command> [in <dir>] on <host> [host ...]
    $me [options] pushnexec <command> [in <dir>] on <host> [host ...]

* Local commands:
    $me [options] exec <command> locally [for <host> [host ...]]

* Common options:
    -l <logdir>	 Logs everything in <logdir>/
    -n <number>	 Number of commands to run simultaneously, default: 1
    -q		 Be quiet, that is don't issue command output on terminal
    -t <seconds> Timeout when running a command, default: 120
    -v		 Show command output on terminal

* Remote command options:
    -k <keyfile> Use <keyfile> when using ssh
    -p <seconds> Ping timeout when testing host, disable with 0, default: 5
    -u <user>	 Use <user> when using ssh, default: \$LOGNAME

* Local command options:
    -s <string>	 Substitute <string> for each host, default: %ARG%

* Note:
    For each <file> or <command> option, if you use "-", then lines from
    stdin will be read.  Empty lines and the ones starting with # will be
    skipped.
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
  so it is considered as a single argument.  Every %ARG% string in the
  command will be replaced by the current <arg>.  This string can be changed
  with the -s option.

Examples:
  $me -n 2 exec "uname -s" on host1 host2 host3 host4

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
use POSIX qw(strftime mkdir :sys_wait_h);
use Errno qw(:POSIX);
use IO::Pipe;
use IO::Select;

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
my $logger_pri='user.err';
my $pingtimeout = 5;
my $parallelism = 1; 
my $verbose = 0;
my $quiet = 0;
my $syslogmsg;
my $timeout = 120;
my $ssh_user = $ENV{'LOGNAME'};
my $ssh_keyfile;
my $subst = $ENV{'SUBST'} ? quotemeta ($ENV{'SUBST'}) : '\%ARG\%';
my $logdir = '';
my $loghandle;
my $outhandle;
my $ssh_opts = '-o StrictHostKeyChecking=no -o PasswordAuthentication=no -o NumberOfPasswordPrompts=0 -q';
my $os;
my $pingcmd;
my $sshcmd;
my $scpcmd;

$|=1;

Getopt::Long::Configure qw(posix_default require_order bundling no_ignore_case);
GetOptions(
	'm=s' => \$syslogmsg,
	'l=s' => \$logdir,
	'p=i' => \$pingtimeout,
	'n=i' => \$parallelism,
	't=i' => \$timeout,
	'u=s' => \$ssh_user,
	'k=s' => \$ssh_keyfile,
	'v' => \$verbose,
	's=s' => \$subst,
	'q' => \$quiet,
	'h' => \&usage,
) or (die $!);

$os = `uname -s`;
chomp $os;
$pingcmd = 'ping -c 1';
if ($os eq 'SunOS') { $pingcmd = 'ping' }

if ($ssh_keyfile) { $ssh_opts .= ' -i '.$ssh_keyfile }
$sshcmd = "ssh -n $ssh_opts";
$scpcmd = "scp $ssh_opts";

if ($logdir and not -d $logdir) {
	if (not mkdir $logdir, 0700) {
		die "Cannot create directory '$logdir': $!";
	}
}

# =-=-=-=-=-=-
# Log routines
# =-=-=-=-=-=-

sub printlog {
	my ($level, $tag, $text) = @_;
	my $now = strftime("[%Y/%m/%d_%H:%M:%S]", localtime);
	if ($verbose or ($quiet and $level <= 0) or (not $quiet and $level <= 1)) {
		print "$now ($tag) $text\n";
	}
	if (defined $loghandle) { print $loghandle "$now ($tag) $text\n" }
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
# This function creates a pipe, forks and sets it as stdout before running the
# child.  The parent process reads the pipe and writes it to the log.
# The return value is the exit status of the executed command.
sub pipedrun {
	my ($timer, $command, $slaveid) = @_;

	# It would be easier to use open()'s pipe feature, but we wouldn't
	# be able to get the return status of the command.
	my $outpipe = new IO::Pipe;
	my $errpipe = new IO::Pipe;
	my $pid = fork;
	if (not defined $pid) {
		logerror($slaveid, "Can't execute command: fork: $!");
		return 300;
	}
	if ($pid == 0) {
		$outpipe->writer();
		$errpipe->writer();
		# Set pipe as stdout.
		my $stdout = \*STDOUT;
		my $stderr = \*STDERR;
		if (not defined $stdout->fdopen($outpipe->fileno, 'w')) {
			logerror($slaveid, "Can't execute command: fdopen for stdout: $!");
			exit 127;
		}
		if (not defined $stderr->fdopen($errpipe->fileno, 'w')) {
			logerror($slaveid, "Can't execute command: fdopen for stderr: $!");
			exit 127;
		}
		# Shutdown a warning from Perl: it yells when something else
		# than "exit" is called after "exec".
		no warnings;
		exec $command;
		logerror($slaveid, "Can't execute command: exec: $!");
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
	while (1) {
		my @ready = $select->can_read();
		foreach my $fh (@ready) {
			my $pfx = '';
			if ($fh == $errpipe) { $pfx = 'ERR: ' }
			while (1) {
				my $line = <$fh>;
				if (not defined $line) { last }
				chomp $line;
				logoutput($slaveid, "$pfx$line");
			}
		}
		if ($lastturn) { last }
		while (1) {
			my $pid2 = waitpid -1, WNOHANG;
			if ($pid2 == $pid) { $status = $? }
			if ($pid2 > 0) { next }
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

	if ($status & 127) {
		logerror($slaveid, "Command killed with signal ".($status & 127));
		return -1;
	}
	return ($status >> 8);
}


sub timedrun {
	my ($timer, $command, $slaveid) = @_;
	my ($start, $end);

	$start = time;
	my $status = &Job::Timed::runSubr($timer, \&pipedrun, @_);
	if (not defined $status) {
		logerror($slaveid, "Job::Timed::runSubr: ".&Job::Timed::error());
		return -2;
	}
	if ($status < 0) { return $status }

	$end = time;
	return $status;
}


# Enclose a command in single quotes.
sub escape {
	my ($s) = @_;

	$s =~ s/'/'\\''/g;
	$s = "'$s'";
	return $s;
}


# If 'host' is false, then this is a local job.
#
#
sub dojob {
	my ($slaveid, $jobid, $job, $jobmax) = @_;
	my $action = $job->{'action'};
	my $command = $job->{'command'};
	my $host = $job->{'host'};
	my $dir = $job->{'dir'};
	my ($push, $exec);
	my $status;
	my $realcommand;

	$SIG{'INT'} = $SIG{'TERM'} = \&Job::Timed::terminate;

	if ($logdir) {
		my $logfile = $host ? $host : $jobid;

		if (not open $loghandle, '>>', "$logdir/$logfile.log") {
			logerror($slaveid, "Cannot open '$logdir/$host' for writing: $!");
			goto OUT;
		}
		if (not open $outhandle, '>>', "$logdir/$logfile.out") {
			logerror($slaveid, "Cannot open '$logdir/$host' for writing: $!");
			goto OUT;
		}
		# Shared among multiple process, so disable buffering.
		$loghandle->autoflush(1);
		$outhandle->autoflush(1);
	}

	# Local run is straightforward.
	if (not $host) {
		# Sanity checks after the parser ensured that the action is 'exec'.
		lognormal($slaveid, "(job:$jobid/$jobmax) exec local: $command");
		if (defined $outhandle) {
			print $outhandle "# exec local: $command\n";
		}
		$status = timedrun($timeout, $command, $slaveid);
		goto OUT;
	}

	lognormal($slaveid, "(job:$jobid/$jobmax) $action \@$host: $command");
	if (defined $outhandle) {
		print $outhandle "# $action \@host: $command\n";
	}
	if ($pingtimeout > 0) {
		logdetail($slaveid, "Pinging $host");
		$status = timedrun($pingtimeout, "$pingcmd $host >/dev/null 2>&1", $slaveid);
		if ($status != 0) {
			logerror($slaveid, "Cannot ping '$host'");
			goto OUT;
		}
	}

	$dir =~ s{/$}{};

	if ($action eq 'pull') {
		my $localfile = $command;
		$localfile =~ s{.*/}{};
		$localfile .= ".$host";

		if (not $dir) { $dir = '.' }
		logdetail($slaveid, "Pulling '$command' from $host into $dir/$localfile");
		$status = timedrun(30, "$scpcmd $ssh_user\@$host:$command $dir/$localfile", $slaveid);
		if ($status != 0) {
			logerror($slaveid, "Cannot pull '$command' to $host");
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
		logdetail($slaveid, "Pushing '$localfile' to $host as '$remotefile'");
		$status = timedrun(30, "$scpcmd $localfile $ssh_user\@$host:$remotefile", $slaveid);
		if ($status != 0) {
			logerror($slaveid, "Cannot push '$localfile' to $host");
			goto OUT;
		}

		if ($action eq 'push') { goto OUT }

		# Only meaningful for 'pushnexec'.
		$realcommand =~ s/^\S+//;
		$realcommand = "chmod +x $remotefile; $remotefile $realcommand; rm -f $remotefile";
	}

	if ($dir) { $realcommand = "cd $dir; $realcommand" }

	$realcommand = escape($realcommand);
	logdetail($slaveid, "Running command");
	$status = timedrun($timeout, "$sshcmd $ssh_user\@$host $realcommand", $slaveid);
	if ($status > 0) {
		logerror($slaveid, "Return status $status");
	}

OUT:
	if (defined $loghandle) { close $loghandle }
	if (defined $outhandle) { close $outhandle }
	return $status;
}


# =-=-=-=-=-=-=-=-=-=
# Command-line parser
# =-=-=-=-=-=-=-=-=-=
#
# Here is the deterministic finite state machine of the command-line grammar.
# It is really to implement once you have this.
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
if ($state != S3 and $state != S5) {
	die "Expecting ".join ('/', @{$expected[$state]})." at word $i";
}
if ($where eq 'local' and $action ne 'exec' and $action ne 'readnexec') {
	die "Cannot push/pull locally";
}

my @commands = ();
if ($what eq '-') {
	my $fh;
	while (defined <STDIN>) {
		chomp;
		if (/^\s*$/) { next }	
		if (/^\s\#/) { next }	
		push @commands, $_;
	}
} else {
	push @commands, $what;
}

my @jobs;
if (@hosts > 0) {
	my %job = ();

	if ($where eq 'local') {	# Substitution
		foreach my $cmd (@commands) {
			foreach my $host (@hosts) {
				my $job = {};
				my $realcmd = $cmd;
				$realcmd =~ s/$subst/$host/g;
				$job->{'action'} = $action;
				$job->{'command'} = $realcmd;
				$job->{'host'} = '';
				push @jobs, $job;
			}
		}
	} else {			# 'remote', Combination
		foreach my $cmd (@commands) {
			foreach my $host (@hosts) {
				my $job = {};
				$job->{'action'} = $action;
				$job->{'command'} = $cmd;
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
	
	foreach my $cmd (@commands) {
		my $job = {};
		$job->{'action'} = $action;
		$job->{'command'} = $cmd;
		$job->{'host'} = '';
		push @jobs, $job;
	}
}

use Data::Dumper; print Dumper(\@jobs)."\n";

$SIG{'INT'} = $SIG{'TERM'} = sub {
	Job::Parallel::terminate();
	if (!Job::Parallel::isChild()) { exit 0 }
};

Job::Parallel::run($parallelism, \&dojob, scalar @hosts, @jobs);