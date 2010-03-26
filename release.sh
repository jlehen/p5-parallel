#!/bin/sh

set -e

modularize() {
	sed '
s/^package Job::/package Job_/
2i \
{
$a \
}
'	"$@"
}


{
	modularize Job/Parallel.pm
	modularize Job/Timed.pm
	sed '
	s/^use Job::/use Job_/
	2i \
	{ \
	package main;
	$a \
	}
	' parallel.pl
} > parallel
chmod +x parallel
echo Creating \'parallel\'.
