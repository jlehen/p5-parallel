#!/bin/sh

set -e

modularize() {
	sed '
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
	2i \
	{ \
	package main;
	$a \
	}
	' parallel.pl
} > parallel
chmod +x parallel
echo Creating \'parallel\'.
