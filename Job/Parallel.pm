#!/usr/bin/perl
#
# Copyright (C) 2008-2012 Jeremie Le Hen <jeremie@le-hen.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

package Job::Parallel;
# $Id: Parallel.pm,v 1.7 2010/02/22 10:47:15 jlh Exp $

=pod

=head1 NAME

Job::Parallel - This module lets you process multiple similar jobs in parallel.

=head1 DESCRIPTION

Run a subroutine in parallel using multiple children process.  Each job will be
provided in turn to the callback, which can then return a value to the calling
process.

This library requires to be shut down properly because it uses IPC sempahores,
which are not garbage collected when the last process exits.  Thus hitting
Ctrl-C is not enough.  For a quick seatbelt, use the following SIGINT handler:

	$SIG{'INT'} = \&Job::Parallel::terminate;

=head1 USAGE

=head2 C<Job::Parallel::run(nslaves, jobfunc_ref, jobargs_ref, jobslist)>

Run a subroutine in multiple subprocesses in parallel, each receiving the
next job to perform successively.  The calling convention is explained
below in CALLBACK SUBROUTINE CALLING CONVENTION.  The return value is
an array of all values returned by the successive calls to your subroutine.
On error, a single undef is returned and you can call the error() function
to get the error string.

=over

=item nslaves

Number of processes to fork to perform jobs.  The maximum value is 65535.

=item jobfunc_ref

Reference to a callback subroutine executed for each job.  Its return value,
if any, is expected to be a scalar and will be provided back to the calling
process.  This subroutine B<MUST> return (put differently, must not call
exit(2) itself), otherwise the controlling process would wait forever.
See also the CALLBACK SUBROUTINE CALLING CONVENTION section below
for more information.

=item jobargs_ref

Reference to an array or a hash containing additional common constant
arguments to the
I<jobfunc> subroutine.  Use I<undef> or I<[]> when you don't need this
feature.

=item jobslist

A list containing a jobs to be passed in turn the I<jobfunc> subroutine.
This can be anything you want, but array and hash references are treated
specially.  See the CALLBACK SUBROUTINE CALLING CONVENTION section below
for more information.

=back

=head2 C<Job::Parallel::terminate()>

If you want to interrupt the tasks before run() returns, you B<MUST>
call this function to destroy the semaphore.  Otherwise you'll have to
do it manually using ipcrm(1).

Usually this function is called to interrupt run() if a signal is received.
In this case it is your duty to catch the signal in order to call this
function.  Calling this function while no jobs are running or inside a
slave process is does nothing.

This function doesn't kill the currently running jobs.  Each child
process will quit as soon as its currently running job terminates.
This function waits for each child before exiting.  However, this
function is not reentrant, therefore you should ensure to block
the signal that triggered it while it is running.

=head2 C<Job::Parallel::isChild()>

This function tells whether you are in the controlling process or in
a slave.  This is especially useful when you set up a signal handler
in which you want to call exit(2).  Indeed a slave process B<MUST NOT>
call exit(2) unexpectedly or the controlling process will wait forever.

=head2 C<Job::Parallel::error()>

This function returns the last error as a string or undef if there was
no error.

=head1 CALLBACK SUBROUTINE CALLING CONVENTION

The I<jobfunc> subroutine is called differently depending on the nature
of I<job> (element from I<jobslist>) and I<staticarg>.  I<slaveId> is
the index of the slave handling the job (starts at 0) and I<jobId> is the
index of the job (starts at 1).

C<jobfunc($slaveId, $jobId, $job, $staticarg)>

=head1 ERROR HANDLING

If you have more than one job you can easily check there was an error by
checking the number of returned values:

	my @results = Job::Parallel::run(10, \&work, @jobs);
	if (@results == 1) {	# You can assert it is undef
		print STDERR Job::Parallel::error()."\n";
	}

In the very unlikely case where you have a single job B<and> your subroutine
may return undef, then you will have to check whether the error() function
returns a false value or not.  The most bullet-proof error handling is:

	my @results = Job::Parallel::run(10, \&work, @jobs);
	if (Job::Parallel::error()) {
		print STDERR Job::Parallel::error()."\n";
	}

=head1 IMPORTANT NOTES

Actually the data is provided to and fro using B<Data::Dumper>, so as long as it
make sense to exchange your data between forked processes, you should be fine.
I haven't tested to provide objects or subroutines, but it should work if there
is no operating-system state involved.

The return value for each job will be enqueued as soon as the callback returns.
This means that the job order in I<joblist> doesn't enforce the result order.
It is up to you to define a way to relate a job with its result if relevant,
for instance by returnun the jobId in the result.

Once the callback returns, the slave calls POSIX::_exit() so END blocks are not
processed.  You B<must not> terminate a slave process inside the subroutine
because the slave has to decrease a semaphore before exiting in order to inform
the controlling process it has done its job.  Otherwise the controlling process
would wait forever.

Another important thing to know about is that all results are kept in
memory while all jobs haven't been achieved.  So avoid returning too large
results, especially if you have a great number of jobs.  Adding yet
another callback to handle the result flow as they come would relieve, but
I haven't needed this so far.

=head1 AUTHOR

This module has been written by Jeremie LE HEN.

=head1 VERSION

$Id: Parallel.pm,v 1.7 2010/02/22 10:47:15 jlh Exp $

=cut

use strict;
use warnings;
use Data::Dumper;
use Errno qw(EINTR EBADF);
use IO::Pipe;
use IO::Handle;
use POSIX qw(_exit :sys_wait_h :signal_h);		# WNOHANG
use IPC::SysV qw(IPC_PRIVATE IPC_RMID);

# Forks as much childs as requested.  Each child is given orders through its
# own "downward" pipe while all childs return their result through a single
# shared "upward" pipe.
#
# Note that it's possible to pass data to childs through their downward pipe.
# Likewise, childs can return data through the upward pipe.  In the latter
# case, it's up to the callback to provide a way to identify which job a result
# belongs to.
# Beware that you should avoid returning big results along with a large number
# of jobs because all of them are stored in memory until the last job
# has been processed.  Use file instead in this case.  This will go away
# once we will have an asynchronous version.
#
# What's really nice with this design is that you just have to close the
# downward pipe and the child exits.  This mean that to terminate all childs
# at once, you just have to empty the "%_downmap" hash from the parent proc,
# which would trigger the garbage collector which in turn will close all
# pipes.

$| = 1;
my $_error;
my $_rdsiz = 1024;
my $_semid = undef;
my %_downmap;		# Map childId to downward pipe.
my $_up;		# Upward pipe.
my $_slaveid = undef;
my $_slavecount = undef;
my $_jobcount = undef;
my %_pidmap;		# Used in parent only.

my $_osigset = POSIX::SigSet->new();	# Backup sigmask.
my $_sigset = POSIX::SigSet->new();	# Filled sigmask.
$_sigset->fillset();

use constant STARTING => 0;
use constant RUNNING => 1;
use constant TERMINATING => 2;
use constant TERMINATED => 3;
my $_state = TERMINATED;

# Debug mask:
# 0x1 - Basic debugging
# 0x2 - Extra debugging
# 0x4 - Error debugging (think error())
# 0x8 - Buffer debugging
# 0x10 - Buffer dumping
# 0x20 - Critical sections
use constant D_BASIC => 0x1;
use constant D_EXTRA => 0x2;
use constant D_ERR => 0x4;
use constant D_BUF => 0x8;
use constant D_BUFDUMP => 0x10;
use constant D_CRIT => 0x20;
my %_debugpfx = (
	0x1 => "basic",
	0x2 => "extra",
	0x4 => "err  ",
	0x8 => "buf  ",
	0x10 => "bufdump",
	0x20 => "crit "
);
my $_debug = 0;

sub error {

	return $_error;
}

sub isChild {

	return defined $_slaveid;
}

sub terminate {

	if (defined $_slaveid) {
		_debug("Slave $_slaveid ($$) early termination requested", D_BASIC);
		return;
	}

	# Checking $_semid below should be enough, but it's more readble
	# this way.
	if ($_state == TERMINATING || $_state == TERMINATED) { return }

	# No child has been created so far.
	if (not defined $_semid) { return }

	$_state = TERMINATING;
}

sub _enterCritical {

	_debug("Master> entering critical section", D_CRIT);
	if (defined sigprocmask(SIG_BLOCK, $_sigset, $_osigset)) { return }
	die "ASSERTION FAILED: cannot block signals: $!";
}

sub _leaveCritical {

	_debug("Master> leaving critical section", D_CRIT);
	if (defined sigprocmask(SIG_SETMASK, $_osigset)) { return }
	die "ASSERTION FAILED: cannot unblock signals: $!";
}

sub _terminate {
	my ($waitchildren) = @_;

	_enterCritical();
	if ($waitchildren and $_state != TERMINATING) {
		die "ASSERTION FAILED: \$_state must be TERMINATING within _terminate(1)";
	}
	# This doesn't look a nasty line, but it closes all downward pipes at
	# once and thus asks all children to terminate.
	%_downmap = ();
	# Wait all children to have released the mutex.
	_debug("Master> waiting all children to terminate", D_BASIC);
	if (not semop $_semid, pack ('s!s!s!', 1, 0, 0)) {
		my $e = "$!";
		_debug("child semaphore wait-for-zero failed: $e", D_ERR);
		$_error = "child semaphore wait-for-zero failed: $e";
		goto ERROR;
	}
	_debug("Master> all children terminated", D_BASIC);
	if ($waitchildren) {
		_debug("Master> collecting zombies", D_EXTRA);
		my $pid;
		while (1) {
			$pid = waitpid -1, 0;
			if ($pid == -1) { last }
			_debug("Master> collected slave $_pidmap{$pid} zombie ($pid)", D_EXTRA);
			delete $_pidmap{$pid};
		}
	}

	if (not defined semctl $_semid, 1, IPC_RMID, 0) {
		my $e = "$!";
		_debug("semaphore array destruction failed: $e", D_ERR);
		$_error = "semaphore array destruction failed: $e";
	}

	# XXX Blocks, why?
	#if ($_state == STARTING) {
		#_debug("Master> collecting ex-starting-children zombies", D_EXTRA);
		#my $pid;
		#while (1) {
			#$pid = waitpid -1, 0;
			#if ($pid == -1) { last }
			#_debug("Master> collected slave $_pidmap{$pid} zombie ($pid)", D_EXTRA);
			#delete $_pidmap{$pid};
		#}
	#}

	$_up = undef;
	$_semid = undef;
	$_state = TERMINATED;
ERROR:
	_leaveCritical();
	return;
}

sub _debug {
	my ($msg, $level) = @_;

	if (not defined $_debugpfx{$level}) {
		die sprintf "Undefined debug level %x", $level;
	}
	if (!($_debug & $level)) { return }
	print "DEBUG$_debugpfx{$level}: $msg\n";
}

sub _dump {
	my ($dumpee) = @_;

	my $dumper = Data::Dumper->new([$dumpee]);
	$dumper->Indent(0);
	$dumper->Purity(1);
	return $dumper->Dump();
}

# Called in case a child dies.
sub _dyingchild {
	semop $_semid, pack ('s!s!s!', 1, -1, 0);
}

# The downward pipe is not shared among multiple childs.  Therefore we are
# sure that there won't be more than one job at time in the pipe.
# So we know that:
#	- Incoming packed format is "N N/a*", respectively job id, job length
#	  and job itself.
#	- At least the leading job id and job length can be read atomically.
#	- Reading a job will consume all data from pipe.
# The packed format of data sent back to master in "n N/a*", respectively
# slave id, result length and result itself (note that the slave id is serialized
# on 16 bits, so there can't be more than 65535 slaves).
sub _child {
	#my ($i, $semid, $up, $down, $jobfunc, $jobargs) = @_;
	my ($up, $down, $jobfunc, $staticargs) = @_;
	my $title = $0;

	# Initially, this variable wasn't global and belonged to run() scope.
	# I had to make it global for terminate(), but I kept the initial
	# interface for _child().  So clean it now to be pedantic.
	undef $_up;

	$0 = "(Slave $_slaveid/$_slavecount, no job) $title";
	_debug("Slave $_slaveid ($$) started", D_BASIC);
	my $rest = '';
	while (1) {
		my ($buf, $nread, $job, $joblen, $jobid);

		$nread = $down->sysread($buf, $_rdsiz);
		if (not defined $nread) {
			if ($! == EINTR) { next }
			_debug("slave $_slaveid: downward pipe read: $!", D_EXTRA);
			last;
		}
		if ($nread == 0) { last } # Downward pipe closed, cleanup and exit.

		$rest .= $buf;

		if (not defined $jobid) {
			($jobid, $rest) = unpack 'N a*', $rest;
		}
		if (not defined $joblen) {
			($joblen, $rest) = unpack 'N a*', $rest;
		}
		if ($joblen > length $rest) {
			_debug("slave $_slaveid: joblen $joblen > length rest ".
			    length ($rest).", need more data", D_BUF);

			next;
		}

		($job, $rest) = unpack "a$joblen a*", $rest;
		if (length $rest != 0) {
			die "ASSERTION FAILED IN CHILD $_slaveid: remaining data (".
			    length ($rest)." bytes) in downward pipe";
		}

		no strict 'vars';
		$job = eval $job;
		use strict 'vars';
		if (not defined $job) {
			die "ASSERTION FAILED IN CHILD $_slaveid: cannot eval input: $@"
		}

		$0 = "(Slave $_slaveid/$_slavecount, doing job $jobid/$_jobcount) $title";
		_debug("Slave $_slaveid ($$) got job $jobid", D_BASIC);

		my $res;
		$res = $jobfunc->($_slaveid, $jobid, $job, $staticargs);

		$0 = "(Slave $_slaveid/$_slavecount, no job) $title";
		my $pack = pack 'n N/a*', $_slaveid, _dump($res);

		_debug("slave $_slaveid returns ".length ($pack)." bytes", D_BUF);

		# Note: DO NOT use flock.  Duplicated file descriptors
		# share the same locks, so those are not usable here.
		_debug("slave $_slaveid: lock upward pipe", D_EXTRA);
		while (1) {
			if (not semop $_semid, pack ('s!s!s!', 0, -1, 0)) {
				if ($! == EINTR) {
					_debug("slave $_slaveid: upward pipe lock interrupted, retrying", D_EXTRA);
					next;
				}
				die "ASSERTION FAILED IN CHILD $_slaveid: couldn't acquire mutex: $!";
			}
			last;
		}

		my $writeerr;
		while (1) {
			# We can't get SIGPIPE/EPIPE, as the upward
			$writeerr = $up->syswrite($pack);
			if (not defined $writeerr) {
				if ($! == EINTR) {
					_debug("slave $_slaveid: upward pipe write interrupted, retrying", D_EXTRA);
					next;
				}
				_debug("slave $_slaveid: upward pipe write: $!", D_EXTRA);
			}
			last;
		}

		_debug("slave $_slaveid: unlock upward pipe", D_EXTRA);
		while (1) {
			if (not semop $_semid, pack ('s!s!s!', 0, 1, 0)) {
				if ($! == EINTR) {
					_debug("slave $_slaveid: upward pipe unlock interrupted, retrying", D_EXTRA);
					next;
				}
				die "ASSERTION FAILED IN CHILD $_slaveid: couldn't release mutex: $!";
			}
			last;
		}

		undef $jobid;
		undef $joblen;
	}
STOPCHILD:
	_debug("slave $_slaveid: stopping", D_EXTRA);
	if (not semop $_semid, pack ('s!s!s!', 1, -1, 0)) {
		die "ASSERTION FAILED IN CHILD $_slaveid: couldn't decrease children semaphore: $!";
	}
	_debug("Slave $_slaveid ($$) stopped", D_BASIC);
}

# run(NSLAVES, Jobfunc_reF, JOBARGS_REF, JOBLIST)
#	NSLAVES: Number of processes to fork.
#	JOBFUNC_REF: Reference to subroutine being executed for each job.
#		     It will be called with the following arguments:
#		         SLAVE_ID, JOB, JOBARGS
#	JOBARGS_REF: Reference to an array or a hash containing additional
#		     arguments for the subroutine common to all jobs.
#	JOBLIST: List of jobs that will be passed to JOBFUNC in turn.  Note
#		 that array and hash references are dereferenced when passed
#		 to the callback.
sub run {
	my ($nslaves, $jobfunc, $jobargs, @jobs) = @_;

	if ($nslaves > 65535) {
		die "ASSERTION FAILED: too many slaves requested ($nslaves)";
	}
	_enterCritical();
	if ($_state != TERMINATED) {
		die "ASSERTION FAILED: state is not TERMINATED ($_state)";
	}

	$_state = STARTING;
	%_pidmap = ();
	$_error = '';
	_leaveCritical();

	$_up = new IO::Pipe;
	if (not $_up) {
		my $e = "$!";
		_debug("upward pipe creation failed: $e", D_ERR);
		$_error = "upward pipe creation failed: $e";
		return undef;
	}

	# Sem #0 is just a mutex to write in the shared "upward" pipe.
	# Sem #1 is the number of children not terminated.   This is
	# mandatory to avoid a small race where the master terminates
	# before the last child and therefore destroys the semaphore before
	# it is released.
	$_semid = semget IPC_PRIVATE, 2, 0600;
	if (not defined $_semid) {
		my $e = "$!";
		_debug("semaphore array creation failed: $e", D_ERR);
		$_error = "semaphore array creation failed: $e";
		return undef;
	}
	my $sem0op = pack ('s!s!s!', 0, 1, 0);
	my $sem1op = pack ('s!s!s!', 1, $nslaves, 0);
	if (not semop $_semid, $sem0op.$sem1op) {
		my $e = "$!";
		_debug("semaphore array initialization failed: $e", D_ERR);
		$_error = "semaphore array initialization failed: $e";
		goto ERROR;
	}

	$_slavecount = $nslaves;
	$_jobcount = @jobs;
	for (my $i = 0; $i < $nslaves; $i++) {
		if ($_state == TERMINATING) {
			_debug("Master> early termination during workers initialization", D_EXTRA);
			_terminate(1);
			goto ENDMASTER;
		}

		my $down = new IO::Pipe;
		if (not $down) {
			my $e = "$!";
			_debug("downward pipe $i creation failed: $e", D_ERR);
			$_error = "downward pipe $i creation failed: $e";
			goto ERROR;
		}

		my $pid = fork;
		if (not defined $pid) {
			my $e = "$!";
			_debug("fork $i failed: $e", D_ERR);
			$_error = "fork $i failed: $e";
			goto ERROR;
		}

		if ($pid) {			# *** PARENT ***
			$down->writer();
			$down->autoflush(1);

			$_downmap{$i} = $down;
			$_pidmap{$pid} = $i;

		} else {			# *** CHILD ***
			$SIG{__DIE__} = \&_dyingchild;

			$_slaveid = $i;
			%_downmap = ();	# Close all other fd.

			$_up->writer();
			$_up->autoflush(1);

			$down->reader();
			$down->autoflush(1);

			#_child($i, $_semid, $_up, $down, $jobfunc, $jobargs);
			_child($_up, $down, $jobfunc, $jobargs);

			# Avoid callind and END block set in the parent process.
			_exit(0);
		}
	}

	#
	# Master's job.
	_enterCritical();
	if ($_state == TERMINATING) {
		_debug("Master> early termination right after workers initialization", D_EXTRA);
		_leaveCritical();
		_terminate(1);
		goto ENDMASTER;
	}
	$_up->reader();
	$_up->autoflush(1);
	$_state = RUNNING;
	_debug("Master> Ready", D_BASIC);
	_leaveCritical();

	my $jobid = 1;

	foreach (keys %_downmap) {
		my $job = shift @jobs;

		if ($_state == TERMINATING) {
			_debug("Master> early termination requested while dispatching initial jobs", D_EXTRA);
			_terminate(1);
			goto ENDMASTER;
		}

		if (defined $job) {
			_debug("Master> initial dispatching job $jobid to slave $_", D_BASIC);
			$_downmap{$_}->syswrite(pack 'N N/a*', $jobid, _dump($job));
			$jobid++;
		} else {
			_debug("Master> shutting down superfluous slave $_", D_BASIC);
			delete $_downmap{$_};
		}
	}

	my $rest = '';			# Buffer of incoming data
	my $i = -1;			# Child index
	my $reslen = -1;		# Job result len
	my $res;			# Job result
	my $readmore = 0;		# Need more data in buffer
	my @results;

	# We should check for "length $rest > 0", but "keys %_downmap" won't be
	# nul if there is still data in $rest anyway.
	# Also we know that all slaves have been provided one job at least,
	# so we can safely expected something back from them.
	while (@jobs or keys %_downmap) {
		my ($buf, $nread);

		if ($_state == TERMINATING) {
			_debug("Master> termination requested before reading upward pipe", D_EXTRA);
			_terminate(1);
			goto ENDMASTER;
		}

		if (length $rest == 0 or $readmore) {
			_debug("Master> reading upward channel (".
			    scalar (keys %_downmap)." childs)", D_BUF);

			while (1) {
				$nread = $_up->sysread($buf, $_rdsiz);
				if (defined $nread) { last }
				# XXX I still don't understand why $! is 0.
				if ($! == 0) { $! = EINTR }
				my $es = "$!";
				my $en = $!;
				_debug("Master> upward pipe read: $es", D_ERR);

				if ($en == EINTR) {
					if ($_state != TERMINATING) {
						_debug("Master> upward pipe read interrupted, retrying", D_EXTRA);
						next;
					}
					_debug("Master> upward pipe read interrupted, termination requested", D_EXTRA);
					_terminate(1);
					goto ENDMASTER;
				}
				_debug("Master> upward pipe read: $es", D_ERR);
				$_error = "upward pipe read: $es";
				goto ERROR;
			}
			if ($nread == 0) {
				# Children shouldn't close the downward pipe.
				_debug("Master> unexpected upward pipe closed", D_ERR);
				$_error = "unexpected upward pipe closed";
				goto ERROR;
			}

			_debug("Master> growing buffer: ".length ($rest).
			    " + $nread -> ".(length ($rest) + $nread)." bytes", D_BUF);

			$rest .= $buf;
		} else {
			_debug("Master> ".length ($rest)." bytes left in buffer", D_BUF);
		}

		if ($i == -1) {
			# n is 16 bits.
			if (length $rest < 2) {
				$readmore = 1;
				next;
			}
			($i, $rest) = unpack 'n a*', $rest;

			_debug("Master> input from slave: $i", D_BUF);
		}
		if ($reslen == -1) {
			# N is 32 bits.
			if (length $rest < 4) {
				$readmore = 1;
				next;
			}
			($reslen, $rest) = unpack 'N a*', $rest;

			_debug("Master> result len: $reslen", D_BUF);
		}
		if ($reslen > length $rest) {
			_debug("Master> not enough data in buffer ($reslen".
			    " expected for slave $i)", D_BUF);
			_debug("Master> buffer dump: <$rest>", D_BUFDUMP);

			$readmore = 1;
			next;
		}
		_debug("Master> buffer dump: <$rest>", D_BUFDUMP);
		($res, $rest) = unpack "a$reslen a*", $rest;

		no strict 'vars';
		$res = eval $res;
		use strict 'vars';
		push @results, $res;
		
		_debug("Master> slave $i has done its job", D_BASIC);

		if ($_state == TERMINATING) {
			_debug("Master> termination requested after slave $i has done its job", D_EXTRA);
			_terminate(1);
			goto ENDMASTER;
		}

		if (@jobs) {
			my $job = shift @jobs;

			_debug("Master> dispatching job $jobid to slave $i (".scalar (@jobs)." jobs remaining)", D_BASIC);

			while (1) {
				my $err = $_downmap{$i}->syswrite(pack 'N N/a*', $jobid, _dump($job));
				if (defined $err) { last }
				my $e = "$!";
				if ($! == EINTR) {
					if ($_state != TERMINATING) {
						_debug("Master> slave $i downward pipe write interrupted, retrying", D_EXTRA);
						next;
					}
					_debug("Master> slave $i downward pipe write interrupted, termination requested", D_EXTRA);
					_terminate(1);
					goto ENDMASTER;
				}
				_debug("Master> cannot write to downward pipe of child $i: $e", D_ERR);
				$_error = "cannot write to downward pipe of child $i: $e";
				goto ERROR;
			}
			$jobid++;
		} else {
			_debug("Master> shutting down slave $i", D_BASIC);
			delete $_downmap{$i};

			while (1) {
				my $pid = wait;
				if ($pid == -1) {
					if ($! == EINTR) {
						if ($_state != TERMINATING) {
							_debug("Master> wait for child interrupted, retrying", D_EXTRA);
							next;
						}
						_debug("Master> wait for child interrupted, termination requested", D_EXTRA);
						_terminate(1);
						goto ENDMASTER;
					}
					my $e = "$!";
					_debug("Master> cannot wait for child: $e", D_ERR);
					$_error = "cannot wait for child $i: $e";
					goto ERROR;
				}
				_debug("Master> collected slave $_pidmap{$pid} zombie ($pid)", D_EXTRA);
				delete $_pidmap{$pid};
				last;
			}
		}

		$i = -1;
		$reslen = -1;
		$readmore = 0;
	}
	
	_terminate(0);
ENDMASTER:
	_debug("Master> Master stopped", D_BASIC);
	return @results;

ERROR:
	# XXX Given all jobs have been executed, we should change the API
	# to return the results and nonetheless flag there was an error.
	_terminate(1);
	return undef;
}

sub setDebug {
	my ($val) = @_;

	$_debug = $val;
}

sub setReadSize {
	my ($val) = @_;

	$_rdsiz = $val;
}

1;
