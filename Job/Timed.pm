#!/usr/bin/perl

package Job::Timed;
# $Id: Timed.pm,v 1.11 2010/02/22 10:47:15 jlh Exp $

=pod

=head1 NAME

Job::Timed - This module lets you run a subroutine in a subprocess or
an external command for a bounded time.

=head1 DESCRIPTION

Run something in a sub-process for a bounded time.  This module only works
on POSIX systems.

This module uses SIGALRM, so don't use it if you already use alarm() in your
program.  You cannot use this module in a threaded environment either.

The process running the job will have its own process group so signals
sent from keyboard won't be dispatched to it.  See the terminate() and
signal() functions.

=head1 USAGE

=over

=item C<Job::Timed::runSubr($timeout, $subr_ref, @argslist)>

Run a subroutine in a subprocess for a limited time.  The return value
is undef if there was an error, or the scalar value returned by the
subroutine.  If your subroutine can return undef, you will have to check
whether the error() function returns a false value or not.

=over

=item $timeout

Number of seconds the command is allowed to run.  0 means to timeout.

=item $subr_ref

Reference to a Perl subroutine to execute in the forked process.  Its
return value, if any, is expected to be a scalar value and will be
returned to the parent process.

=item @argslist

Arguments for the subroutine to run.

=back

Note: There will be one process forked to run the subroutine.

=item C<Job::Timed::runCommand($timeout, $command)>

Run an external command for a limited time.
The return value is undef if there was a known error, in which case you can
call error() to know the reason.
Otherwise the status obtained from the wait(2) system call is returned.
Note that if the command couldn't be run the exit status will be 127,
but beware that this might overlap with your command's own exit status.
 
=over

=item $timeout

Number of seconds the command is allowed to run.  0 means to timeout.

=item $command

External command to run.  If it contains shell meta-characters, it will be
executed through sh(1), otherwise it will be executed directly.

=back

This method uses C<runSubr()> internally.  It forks a child monitor process
which will in turn run the actual command, in order to be able to return the
exit status of the command to the parent process through the expected channel.

Note: There will be two processes forked to run the command.  If sh(s) is used
to run your command, there will be three processes forked.

=item C<Job::Timed::terminate()>

This function ought be called in signal handlers in order to destroy
the job being run since the signal won't be dispatched to the job because
it has its own process group.  Calling this function while no jobs is
running or inside the sub-process does nothing.

=item C<Job::Timed::signal($signal)>

Send the signal to the running job.  This function ought be called in
signal handlers.  (The signal won't be automatically dispatched to the
job because it has its own process group.)

=item C<Job::Timed::status()>

Return the status of the last job.
Status code:

=over

=item B<0>

The job terminated without error.

=item B<-1>

The job could not be executed because of an internal error.  You ought
call error() to get more detailled informations.

=item B<-2>

The job has been too long to complete.

=item B<-3>

Job::Timed::terminate() has been used.

=item B<<< >0 >>>

The child process ended with a signal or with a non-zero status.  This is
the return status of the process.

=back

=item C<Job::Timed::error()>

Return a description of the last error, if any.
Error strings:

=over

=item B<Subroutine terminated ...>

=item B<Command terminated ...>

Job::Timed::terminate() has been used.

=item B<Subroutine timeout ... >

=item B<Command timeout ...>

The job has been too long to complete.

=item B<Subroutine failed ...>

=item B<Command failed ...>

Some error occured that prevented the job from being run.

=item B<Subroutine ended ...>

=item B<Command ended ...>

The child process (child monitor for runCommand() or the child running the
subroutine itself for runSubr()) ended but with a signal or a non-zero exit
status.  This error should be rare if you use runSubr() with a simple
subroutine or runCommand().  See the ERROR HANDLING section below though.

=back

=back

=head1 ERROR HANDLING

Most of the time you can check whether undef is returned or not.
Nonetheless this may not be acurate if you use runSubr() with a subroutine
which can return an undefined value as well.  The most bullet-proof error
handling is:

	my $result = Job::Timed::runSubr(10, \&subr);
	#my $result = Job::Timed::runCommand(10, "...");
	if (Job::Timed::error()) {
		print STDERR Job::Timed::error()."\n";
	}

Also, if you decide to use your own subroutine to run your commands, you may
trigger the "Subroutine ended" error despite there was no actual error.  It's
just that the child process wherein your subroutine was runnig exited with a
signal or with a non-zero status.  It's up to you to know how to handle this.

=head1 AUTHOR

Jeremie Le Hen < jeremie at le-hen org >

=head1 VERSION

$Id: Timed.pm,v 1.11 2010/02/22 10:47:15 jlh Exp $

=cut

use strict;
use warnings;
use Data::Dumper;
use Errno qw(EINTR);
use IO::Pipe;
use IO::Handle;
use POSIX qw(:sys_wait_h _exit);

my ($_error, $_status);
my ($_pid, $_timeo, $_what, $_command, $_oAlrmHandler);

sub status {

	return $_status;
}

sub error {

	return $_error;
}

sub _buildError {

	$_pid = undef;
	($_status, $_error) = @_;
	if ($_command ne '') { $_error .= ": $_command" }
	return undef;
}

sub _dump {
	my ($dumpee) = @_;

	my $dumper = Data::Dumper->new([$dumpee]);
	$dumper->Indent(0);
	$dumper->Purity(1);
	return $dumper->Dump();
}

sub signal {
	my ($s) = @_;

	if (!$_pid) { return }

	kill $s, -$_pid;
}

sub terminate {
	if (!$_pid) { return }

	kill 9, -$_pid;
	# XXX Send SIGCONT in case the subprocess has been stopped.  It won't
	# honor SIGKILL otherwise.
	kill 18, -$_pid;

	_buildError(-3, "$_what terminated");
}

sub _alrm {
	terminate();
	_buildError(-2, "$_what timeout (${_timeo}s)");

	if (ref $_oAlrmHandler eq 'CODE') {
		#print "DEBUG($$): Running old handler for ALRM\n";
		$_oAlrmHandler->(@_)
	}
}

sub _commandSubr {
	my ($cmd) = @_;

	my $pid = fork;
	# Exiting with a non-zero here will issue an error in the parent
	# process.
	if (not defined $pid) { _exit(127) }
	if ($pid == 0) {
		# Perl will wrongly issue a warning if "exit" is not called
		# after "exec".
		no warnings;
		exec $cmd;
		# We have already forked, so this will be returned as if
		# the command has been executed but returned status 127.
		_exit(127);
	}

	# Should not fail.
	waitpid $pid, 0;

	return $?;
}

sub _run {
	my ($timeo, $subr, @args) = @_;
	$_timeo = $timeo;

	my $pipe = new IO::Pipe;
	if (not $pipe) {
		return _buildError(-1, "$_what failed (pipe creation failed: $!)");
	}

	# Cache PID in a local variable because terminate() will undefine it
	# through _buildError().
	my $pid = $_pid = fork;
	if (not defined $pid) {
		return _buildError(-1, "$_what failed (failed to fork: $!)");
	}

	if ($pid == 0) {
		# Changing process group is required so the child is not killed
		# when Ctrl-C is hit and we can use the signal handler to 1)
		# kill it and 2) return an error.  Otherwise, even if the
		# caller traps the signal, the child would be killed anyway.
		setpgrp 0, 0;
		$pipe->writer();
		$pipe->autoflush(1);
		$0 = "(Timeout after ${timeo}s) $0";
		my $result = $subr->(@args);
		$pipe->syswrite(_dump($result));
		$pipe->close;
		_exit(0);
	}

	$pipe->reader();
	$_status = 0;
	$_error = '';

	#$Data::Dumper::Indent = 0;
	#my $msg = "DEBUG($$)/INIT:";
	if (defined $SIG{'ALRM'}) {
		$_oAlrmHandler = $SIG{'ALRM'};
	} else {
		$_oAlrmHandler = 'DEFAULT';
	}
	#$msg .= " $s => ".Dumper($_oAlrmHandler);
	#print "$msg\n";

	local $SIG{'ALRM'} = \&_alrm;

	#$msg = "DEBUG($$):RUN";
	#foreach my $s qw(ALRM TERM INT) {
	#	$msg .= " $s => ".Dumper($SIG{$s});
	#}
	#print "$msg\n";

	alarm $timeo;

	my $wholebuf = '';
	my $buf;
	while (1) {
		my $nread = $pipe->read($buf, 8192);
		if (not defined $nread) {
			if ($! == EINTR) { next }
			my $errmsg = "$!";
			$pipe->close;
			return _buildError(-1, "$_what failed (pipe read: $errmsg)");
		}
		if ($nread == 0) { last }
		$wholebuf .= $buf;
	}
	$pipe->close;
	my $wpid = waitpid $pid, 0;
	my $status = $?;

	alarm 0;
	$_pid = undef;
	
	no strict 'vars';
	my $result = eval $wholebuf;
	use strict 'vars';

	#$msg = "DEBUG($$):END";
	#foreach my $s qw(ALRM TERM INT) {
	#	$msg .= " $s => ".Dumper($SIG{$s});
	#}
	#print "$msg\n";

	# A signal handler has been called.
	if ($_error ne '') { return undef }

	if ($status & 127) {
		return _buildError($status, "$_what ended (child died with ".
		    "signal ".($status & 127).")");
	}

	if ($status >> 8 != 0) {
		return _buildError($status, "$_what ended (child exited with ".
		    "value ".($status >> 8).")");
	}

	if (not defined $result) { $result = '' };

	return $result;
}

sub runSubr {
	my ($timeo, $subr, @args) = @_;

	$_what = "Subroutine";
	$_command = '';
	return _run($timeo, $subr, @args);
}

sub runCommand {
	my ($timeo, $command) = @_;

	$_what = "Command";
	$_command = $command;
	return _run($timeo, \&_commandSubr, $command);
}


if (defined $ENV{'windir'}) {
	die "Sorry, this module is not supported on Win32";
}

1;
